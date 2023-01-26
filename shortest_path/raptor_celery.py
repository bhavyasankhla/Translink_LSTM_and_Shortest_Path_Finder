from spark_celery import SparkCeleryApp, SparkCeleryTask, cache, main
from pyspark.sql import functions as F, types, Window
from datetime import datetime, timedelta
from math import inf
from kombu import Queue
import os
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# Spark Celery URLs for the Cluster.
BROKER_URL = 'amqp://myuser:mypassword@localhost:5672/myvhost'
BACKEND_URL = 'rpc://'

# Builds the shared Spark configuration used by Celery workers.
# Reference: https://github.com/gregbaker/spark-celery/blob/master/demo.py


def sparkconfig_builder():
    from pyspark import SparkConf
    return SparkConf().setAppName('SparkCeleryTask') \
        .set('spark.checkpoint.compress', 'true') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.dynamicAllocation.schedulerBacklogTimeout', 1) \
        .set('spark.dynamicAllocation.minExecutors', 1) \
        .set('spark.dynamicAllocation.executorIdleTimeout', 20) \
        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', 60)


# Create the SparkCeleryApp instance.
app = SparkCeleryApp(broker=BROKER_URL, backend=BACKEND_URL,
                     sparkconf_builder=sparkconfig_builder)
priority = int(os.environ.get('CONSUMER_PRIORITY', '10'))
app.conf['task_queues'] = (
    Queue('celery', consumer_arguments={'x-priority': priority}),
)

# The main class for the Spark job
# I tried to keep this in a separate file but could not figure out how to access the Spark Context in the other file.


class Raptor(SparkCeleryTask):
    # The name of the task
    name = 'Raptor'

    def define_stop_times_schema(self):
        stop_times_scehma = types.StructType([
            types.StructField('trip_id', types.StringType()),
            types.StructField('arrival_time', types.StringType()),
            types.StructField('departure_time', types.StringType()),
            types.StructField('stop_id', types.StringType()),
            types.StructField('stop_sequence', types.IntegerType()),
            types.StructField('route_id', types.StringType()),
        ])
        return stop_times_scehma

    def define_stops_schema(self):
        stops_schema = types.StructType([
            types.StructField('stop_id', types.IntegerType()),
            types.StructField('stop_name', types.StringType())
        ])
        return stops_schema

    def define_transfer_schema(self):
        transfer_schema = types.StructType([
            types.StructField('from_stop_id', types.StringType()),
            types.StructField('to_stop_id', types.StringType()),
            types.StructField('transfer_time', types.FloatType())
        ])
        return transfer_schema

    @cache
    def read_files(self):
        transfers = app.spark.read.parquet(f'{self.input_directory}/{self.day}/transfers',
                                           schema=self.define_transfer_schema())
        stop_times = app.spark.read.parquet(f'{self.input_directory}/{self.day}/stop_times',
                                            schema=self.define_stop_times_schema())
        stops = app.spark.read.parquet(f'{self.input_directory}/{self.day}/stops',
                                       schema=self.define_stops_schema())
        transfers = transfers.cache()
        # Count to force caching of the entire dataframe
        transfers.count()
        stops = stops.cache()
        # Count to force caching of the entire dataframe
        stops.count()
        return transfers, stop_times, stops

    # Filter stop times to only include those that are within 1 hour of the departure time and before midnight
    @cache
    def filter_stop_times(self, stop_times, departure_time):
        stop_times = stop_times.filter(stop_times.arrival_time < '24:00:00')
        one_hour_from_departure = format(datetime.strptime(
            departure_time, '%H:%M') + timedelta(hours=1), '%H:%M')
        stop_times = stop_times.filter(stop_times.arrival_time > departure_time).filter(
            stop_times.arrival_time < one_hour_from_departure)
        stop_times = stop_times.cache()
        # Count to force caching of the entire dataframe
        stop_times.count()
        return stop_times

    # Initializes the stops table with values for the first round
    def initialize_stops(self, stops, origin):
        # Add empty array of length max_transfers to each stop to store earliest arrival time (EAT) for each round
        modified_stops = stops.withColumn(
            'eat', F.array([F.lit(inf)] * self.max_transfers))
        # Set origin to have EAT as 0 initially for the first round
        modified_stops = modified_stops.withColumn('eat', F.when(modified_stops.stop_id == origin, F.array(
            [F.lit(0)] + [F.lit(inf)] * (self.max_transfers - 1))).otherwise(modified_stops.eat))
        # Flag to mark if stop has improved this round
        modified_stops = modified_stops.withColumn('improved', F.lit(False))
        # Stores the last round this stop was improved in
        modified_stops = modified_stops.withColumn('improved_round', F.lit(-1))
        # Stores the parent trip from where this stop was reached
        modified_stops = modified_stops.withColumn('parent_trip', F.lit(-1))
        # Stores the parent stop from where the parent trip started
        modified_stops = modified_stops.withColumn('parent_stop', F.lit(-1))
        # Initially mark origin as improved
        modified_stops = modified_stops.withColumn('improved', F.when(
            modified_stops.stop_id == origin, F.lit(True)).otherwise(modified_stops.improved))
        modified_stops = modified_stops.select(
            "stop_id", "eat", "improved", "improved_round", "parent_trip", "parent_stop", "stop_name")
        return modified_stops

    # Find all stops that have improved in this round
    def find_marked_stops(self, modified_stops):
        # Finds the minimum EAT for each stop considering all rounds calculated
        modified_stops = modified_stops.withColumn(
            'min_eat', F.array_min(modified_stops.eat))
        # Get all stops that have improved
        marked_stops = modified_stops.filter(modified_stops.improved == True)
        # Unmark all stops
        modified_stops = modified_stops.withColumn('improved', F.lit(False))
        return modified_stops, marked_stops

    # Find all trips that can be taken from the marked stops
    def find_trips(self, marked_stops, stop_times, round, departure_time):
        # Join marked stops with stop times to find all trips that can be taken from the marked stops
        all_trips = stop_times.join(marked_stops.hint('broadcast'), ['stop_id']).select(
            "trip_id", "stop_id", "eat", "stop_sequence", "departure_time", "route_id", "parent_trip")
        # Calculate travel time for each trip
        if (round == 0):
            # If this is the first round, then we don't need to consider the prior travel time
            all_trips = all_trips.withColumn('travel_time', F.unix_timestamp(all_trips.departure_time, 'HH:mm:ss') - F.unix_timestamp(
                F.lit(departure_time), 'HH:mm'))
            all_trips = all_trips.filter(F.col("travel_time") >= 0)
        else:
            # If this is not the first round, then we need to consider the prior travel time so we subtract the prior travel time
            all_trips = all_trips.withColumn('travel_time', F.unix_timestamp(
                all_trips.departure_time, 'HH:mm:ss') - F.unix_timestamp(F.lit(departure_time), 'HH:mm') - all_trips.eat[round - 1])
            # This filter is to retain only trips that start after we arrive at a stop
            all_trips = all_trips.filter(F.col("travel_time") >= 0)
            # Add the prior travel time back to get the total travel time to reach this stop
            all_trips = all_trips.withColumn('travel_time', F.col(
                "travel_time") + all_trips.eat[round - 1])
        all_trips = all_trips.select(
            "trip_id", "stop_id", "stop_sequence", "departure_time", "travel_time", "route_id", "parent_trip")
        return all_trips

    # Find the earliest trips from each route
    def find_earliest_trips(self, all_trips):
        # Create a window that partitions by stop_id and orders by travel_time
        all_trips.sort('stop_id', 'trip_id').write.csv(
            f'raptor_debug/all_trips/round_{self.round}', mode='overwrite')
        travel_time_window = Window.partitionBy(
            'parent_trip', 'route_id').orderBy('travel_time')
        # Find the earliest trip for each stop
        earliest_trips = all_trips.withColumn(
            'earliest_trip_id', F.first('trip_id').over(travel_time_window))
        earliest_trips = earliest_trips.withColumn(
            'hop_on_stop_id', F.first('stop_id').over(travel_time_window))
        # Find the earliest stop in the trip
        earliest_trips = earliest_trips.withColumn('check_stop_sequence', F.first(
            'stop_sequence').over(travel_time_window))
        earliest_trips = earliest_trips.withColumn('earliest_trip_time', F.first(
            'departure_time').over(travel_time_window))
        earliest_trips = earliest_trips.withColumn(
            'fastest_travel_time', F.first('travel_time').over(travel_time_window))
        earliest_trips = earliest_trips.select(
            'check_stop_sequence', 'hop_on_stop_id', 'earliest_trip_id', 'earliest_trip_time', 'fastest_travel_time')
        earliest_trips = earliest_trips.distinct()
        earliest_trips.write.csv(
            f'raptor_debug/earliest_trips/round_{self.round}', mode='overwrite')
        return earliest_trips

    # Find all trips that can be walked to from the stops
    def find_footpath_trips(self, earliest_trips, transfers, modified_stops, trip_columns):
        footpath_trips = earliest_trips.drop("stop_id")
        # Join with the preprocessed transfers table that contains all footpath trips within 100 metres
        footpath_trips = footpath_trips.join(transfers.hint(
            'broadcast'), earliest_trips.hop_on_stop_id == transfers.from_stop_id)
        # Add a 'W' to the trip ID to distinguish footpath trips
        footpath_trips = footpath_trips.withColumn(
            'earliest_trip_id', F.concat(footpath_trips.earliest_trip_id, F.lit("W")))
        # Rename columns to match the regular trip schema
        footpath_trips = footpath_trips.drop(
            "check_stop_sequence", "from_stop_id").withColumnRenamed("to_stop_id", "stop_id")
        # Join with the modified stops table to get the EAT for the footpath trip
        footpath_trips = modified_stops.join(footpath_trips.hint('broadcast'), [
                                             "stop_id"]).withColumn('new_travel_time', F.col('fastest_travel_time'))
        footpath_trips = footpath_trips.select(trip_columns)
        return footpath_trips

    def find_filtered_trips(self, earliest_trips, modified_stops, stop_times, trip_columns):
        # Join with the stop times table to find all stops that can be reached from the earliest trips
        filtered_trips = stop_times.join(earliest_trips.hint(
            'broadcast'), earliest_trips.earliest_trip_id == stop_times.trip_id)
        # Filter out stops that are before the hop on stop in the trip
        filtered_trips = filtered_trips.filter(
            F.col("stop_sequence") > F.col("check_stop_sequence"))
        # Drop columns that are not needed
        filtered_trips = filtered_trips.drop("check_stop_sequence")
        # Join with the modified stops table to get the travel_time for each stop in each trip
        filtered_trips = modified_stops.join(
            filtered_trips.hint('broadcast'), ["stop_id"])
        filtered_trips = filtered_trips.withColumn('new_travel_time', F.unix_timestamp(
            filtered_trips.departure_time, 'HH:mm:ss') - F.unix_timestamp(filtered_trips.earliest_trip_time, 'HH:mm:ss') + F.col('fastest_travel_time'))
        # Set transfer time to infinity for trips that are not footpath trips
        filtered_trips = filtered_trips.withColumn('transfer_time', F.lit(inf))
        filtered_trips = filtered_trips.select(trip_columns)
        return filtered_trips

    # Calculate the EAT for each trip
    def calculate_eat(self, filtered_trips, footpath_trips, round):
        # Merge the footpath trips and normal trips
        merged_trips = filtered_trips.union(footpath_trips)
        # Calculate the new EAT for each trip
        if (round != 0):
            # If this is not the first round, add the prior EAT to the travel time and
            # take the minimum of the new EAT, the prior EAT and a possible transfer time
            merged_trips = merged_trips.withColumn('new_eat', F.array_min(F.array(
                merged_trips.new_travel_time, F.element_at(merged_trips.eat, F.lit(round + 1)), F.element_at(merged_trips.eat, F.lit(round)) + merged_trips.transfer_time)))
        else:
            # If this is the first round, take the minimum of the new EAT and a possible transfer time
            merged_trips = merged_trips.withColumn('new_eat', F.array_min(F.array(
                merged_trips.new_travel_time, F.element_at(merged_trips.eat, F.lit(round + 1)), merged_trips.transfer_time)))
        # Add a column to indicate if the new EAT has improved over already stored EAT
        merged_trips = merged_trips.withColumn('new_improved', F.when(
            merged_trips.new_eat < merged_trips.eat[round], F.lit(True)).otherwise(F.lit(False)))

        if (round != 0):
            # If this is not the first round, add a column to indicate if the new EAT has improved over the previous round
            merged_trips = merged_trips.withColumn('improved_over_previous', F.when(
                merged_trips.new_eat < F.array_min(merged_trips.eat), F.lit(True)).otherwise(F.lit(False)))
        else:
            # If this is the first round, add a column to indicate if the new EAT has improved over infinity
            merged_trips = merged_trips.withColumn('improved_over_previous', F.when(
                merged_trips.new_eat != inf, F.lit(True)).otherwise(F.lit(False)))
        # Replace new EAT value in the array column using a UDF
        array_replace = F.udf(
            lambda arr, idx, val: arr[:idx] + [val] + arr[idx + 1:], types.ArrayType(types.FloatType()))
        merged_trips = merged_trips.withColumn('new_eat', array_replace(
            merged_trips.eat, F.lit(round), merged_trips.new_eat))
        return merged_trips

    def check_improvement(self, merged_trips, modified_stops, round):
        # Join the merged trips with the modified stops table
        modified_stops = modified_stops.join(merged_trips.select('stop_id', 'new_eat', 'new_improved', 'improved_over_previous', F.col(
            'hop_on_stop_id').alias(f"new_parent_stop"), F.col('earliest_trip_id').alias(f"new_parent_trip")).hint('broadcast'), ['stop_id'], how='left')
        # If the new EAT has improved, replace the old EAT with the new EAT
        modified_stops = modified_stops.withColumn('eat', F.when(
            modified_stops.new_improved == True, modified_stops.new_eat).otherwise(modified_stops.eat))
        # If the new EAT has improved, set the improved flag to true
        modified_stops = modified_stops.withColumn('improved', F.when(
            modified_stops.new_improved == True, F.lit(True)).otherwise(modified_stops.improved))
        # If the new EAT has improved over the previous round, set the improved_round to the current round
        modified_stops = modified_stops.withColumn('improved_round', F.when(
            modified_stops.improved_over_previous == True, F.lit(round)).otherwise(modified_stops.improved_round))
        return modified_stops

    def store_parents(self, improved_stops):
        # If the new EAT has improved, replace the old parent trip and stop with the new parent trip and stop
        improved_stops = improved_stops.withColumn('parent_trip', F.when(improved_stops.improved_over_previous == True, F.col(
            "new_parent_trip")).otherwise(improved_stops.parent_trip))
        improved_stops = improved_stops.withColumn('parent_stop', F.when(improved_stops.improved_over_previous == True, F.col(
            "new_parent_stop")).otherwise(improved_stops.parent_stop))
        # Drop the columns that are no longer needed
        improved_stops = improved_stops.drop(
            'new_parent_trip', 'new_parent_stop', 'new_improved', 'new_eat', 'improved_over_previous')
        return improved_stops

    def group_all_stops(self, modified_stops):
        # Calculate min_eat for each stop
        modified_stops = modified_stops.withColumn(
            'min_eat', F.array_min(modified_stops.eat))
        # Filter out stops that have not improved ever
        modified_stops = modified_stops.filter(modified_stops.min_eat != inf)
        # Group by stop_id, improved_round, parent_trip, and stop_name
        modified_stops = modified_stops.groupBy(
            'stop_id', 'improved_round', 'parent_trip', 'stop_name')
        # Aggregate min_eat and parent_stop
        modified_stops = modified_stops.agg(F.min('min_eat').alias("min_eat"),
                                            F.first('parent_stop').alias("parent_stop"))
        modified_stops.write.csv(
            f'raptor_debug/modified_stops', mode='overwrite')
        return modified_stops

    def find_destination_trips(self, modified_stops, destinations):
        # Filter out stops that are not destinations
        destination_trips = modified_stops.filter(
            modified_stops.stop_id.isin(destinations))
        return destination_trips

    def get_result(self, modified_stops, destination_trips, stops):
        round = self.max_transfers - 1
        # Get entire trip path for each destination
        while (round > 0):
            parent_stops = modified_stops.filter(
                modified_stops.improved_round == F.lit(round-1))
            parent_stops = parent_stops.select(modified_stops.stop_id.alias(f"stop_{round}"), modified_stops.parent_trip.alias(
                f"parent_trip_{round}"), modified_stops.parent_stop.alias(f"parent_stop_{round}"), modified_stops.stop_name.alias(f"stop_name_{round}"))
            destination_trips = destination_trips.join(
                parent_stops, destination_trips.parent_stop == parent_stops[f"stop_{round}"], how="left")
            destination_trips = destination_trips.drop(
                "parent_stop").withColumnRenamed(f"parent_stop_{round}", "parent_stop")
            round -= 1
        # Get origin stop name
        origin_stop_name = stops.filter(
            stops.stop_id == self.origin).select("stop_name").collect()[0][0]
        destination_trips = destination_trips.withColumnRenamed("parent_stop", "stop_0").withColumnRenamed(
            "parent_trip", f"parent_trip_{self.max_transfers}").withColumnRenamed("stop_name", f"stop_name_{self.max_transfers}")
        # Create array columns for trip, hop_names, hops, and trip_time
        destination_trips = destination_trips.withColumn("trip", F.array(
            [f"parent_trip_{x}" for x in range(1, self.max_transfers + 1)]))
        destination_trips = destination_trips.withColumn("hop_names", F.array([F.lit(
            origin_stop_name)] + [f"stop_name_{x}" for x in range(1, self.max_transfers + 1)]))
        destination_trips = destination_trips.withColumn("hops", F.array(
            [F.lit(self.origin)] + [f"stop_{x}" for x in range(1, self.max_transfers)]))
        # Calculate trip time in minutes
        destination_trips = destination_trips.withColumn(
            "trip_time", (F.col("min_eat") / 60).cast("int"))
        # Select columns of interest
        destination_trips = destination_trips.select(
            "stop_id", "trip_time", "improved_round", "trip", "hops", "hop_names")
        destination_trips = destination_trips.groupBy("stop_id", "trip_time", "improved_round").agg(
            F.first("trip").alias("trip"), F.first("hops").alias("hops"), F.first("hop_names").alias("hop_names"))
        destination_trips.withColumn("trip", F.col("trip").cast("string")).withColumn("hops", F.col("hops").cast(
            "string")).withColumn("hop_names", F.col("hop_names").cast("string")).write.csv(f'raptor_debug/result', mode='overwrite')
        return destination_trips

    def run(self, *args):
        app.spark.sparkContext.setCheckpointDir("raptor_checkpoints")
        self.day = args[0]
        self.departure_time = args[1]
        self.origin = args[2]
        self.destinations = args[3]
        self.input_directory = args[4]
        # Max value the cluster can handle, any more transfers and it runs out of memory.
        # It should scale if there's more memory or nodes
        self.max_transfers = 3
        # Read files
        transfers, stop_times, stops = self.read_files()
        # Check if origin exists
        origin_stop_exists = stops.filter(stops.stop_id == self.origin).count()
        if (not origin_stop_exists):
            return []
        stop_times = self.filter_stop_times(stop_times, self.departure_time)
        modified_stops = self.initialize_stops(stops, self.origin)
        for round in range(self.max_transfers):
            self.round = round
            modified_stops, marked_stops = self.find_marked_stops(
                modified_stops)
            all_trips = self.find_trips(
                marked_stops, stop_times, round, self.departure_time)
            earliest_trips = self.find_earliest_trips(all_trips)
            trip_columns = ["new_travel_time", "eat", "stop_id",
                            "hop_on_stop_id", "earliest_trip_id", "transfer_time"]
            footpath_trips = self.find_footpath_trips(
                earliest_trips, transfers, modified_stops, trip_columns)
            filtered_trips = self.find_filtered_trips(
                earliest_trips, modified_stops, stop_times, trip_columns)
            merged_trips = self.calculate_eat(
                filtered_trips, footpath_trips, round)
            improved_stops = self.check_improvement(
                merged_trips, modified_stops, round)
            modified_stops = self.store_parents(improved_stops)
            modified_stops = modified_stops.checkpoint()
        modified_stops = self.group_all_stops(modified_stops)
        destination_trips = self.find_destination_trips(
            modified_stops, self.destinations)
        result = self.get_result(modified_stops, destination_trips, stops)
        return result.collect()


tasks = app.register_task(Raptor())

if __name__ == '__main__':
    # When called as a worker, run as a worker.
    main()
