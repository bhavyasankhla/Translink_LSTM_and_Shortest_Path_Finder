from pyspark.sql import SparkSession, functions, types
from math import radians, cos, sin, asin, sqrt

import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def define_calendar_schema():
    calendar_schema = types.StructType([
        types.StructField('service_id', types.StringType()),
        types.StructField('monday', types.IntegerType()),
        types.StructField('tuesday', types.IntegerType()),
        types.StructField('wednesday', types.IntegerType()),
        types.StructField('thursday', types.IntegerType()),
        types.StructField('friday', types.IntegerType()),
        types.StructField('saturday', types.IntegerType()),
        types.StructField('sunday', types.IntegerType()),
        types.StructField('start_date', types.LongType()),
        types.StructField('end_date', types.LongType())
    ])
    return calendar_schema
    
def define_trips_schema():
    trips_schema = types.StructType([
        types.StructField('route_id', types.StringType()),
        types.StructField('service_id', types.StringType()),
        types.StructField('trip_id', types.StringType()),
        types.StructField('trip_headsign', types.StringType()),
        types.StructField('trip_short_name', types.StringType()),
        types.StructField('direction_id', types.IntegerType()),
        types.StructField('block_id', types.IntegerType()),
        types.StructField('shape_id', types.StringType()),
        types.StructField('wheelchair_accessible', types.IntegerType()),
        types.StructField('bikes_allowed', types.IntegerType()),
    ])
    return trips_schema

def define_stop_times_schema():
    stop_times_scehma = types.StructType([
        types.StructField('trip_id', types.StringType()),
        types.StructField('arrival_time', types.StringType()),
        types.StructField('departure_time', types.StringType()),
        types.StructField('stop_id', types.StringType()),
        types.StructField('stop_sequence', types.IntegerType()),
        types.StructField('stop_headsign', types.StringType()),
        types.StructField('pickup_type', types.IntegerType()),
        types.StructField('drop_off_type', types.IntegerType()),
        types.StructField('shape_dist_traveled', types.FloatType())
    ])
    return stop_times_scehma


def define_stops_schema():
    stops_schema = types.StructType([
        types.StructField('stop_id', types.IntegerType()),
        types.StructField('stop_code', types.StringType()),
        types.StructField('stop_name', types.StringType()),
        types.StructField('stop_desc', types.StringType()),
        types.StructField('stop_lat', types.FloatType()),
        types.StructField('stop_lon', types.FloatType()),
        types.StructField('zone_id', types.StringType()),
        types.StructField('stop_url', types.StringType()),
        types.StructField('location_type', types.IntegerType()),
        types.StructField('parent_station', types.StringType())
    ])
    return stops_schema

def create_views(calendar_df, trips_df, stop_times_df, stops_df):
    calendar_df.createOrReplaceTempView('calendar')
    trips_df.createOrReplaceTempView('trips')
    stop_times_df.createOrReplaceTempView('stop_times')
    stops_df.createOrReplaceTempView('stops')

def get_filtered_trips(day):
    filtered_trips = spark.sql(f"SELECT route_id, trip_id FROM trips WHERE service_id IN (SELECT service_id FROM calendar)")
    filtered_trips.createOrReplaceTempView('filtered_trips')
    return filtered_trips

def get_filtered_stop_times(day, trips, partitions):
    filtered_stop_times = spark.sql(f"SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence FROM stop_times WHERE trip_id IN (SELECT trip_id FROM filtered_trips)")
    # Filter timestamps more than 24 hours
    filtered_stop_times = filtered_stop_times.filter(filtered_stop_times.arrival_time < '24:00:00')
    filtered_stop_times = filtered_stop_times.join(trips.select("trip_id", "route_id"), ["trip_id"])
    hash_times = filtered_stop_times.orderBy(['trip_id', 'stop_sequence']).select('stop_sequence', 'trip_id') \
                    .groupBy('trip_id') \
                    .agg(functions.concat_ws("", functions.collect_list("stop_sequence")).alias("stop_sequence")) \
                    .withColumn('stop_sequence_hash', functions.hash('stop_sequence')).drop('stop_sequence')
    filtered_stop_times = filtered_stop_times.join(hash_times, ['trip_id'])
    filtered_stop_times = filtered_stop_times.withColumn('route_id', functions.concat('route_id', functions.lit('--'), 'stop_sequence_hash')).drop('stop_sequence_hash')
    filtered_stop_times.createOrReplaceTempView('filtered_stop_times')
    filtered_stop_times = filtered_stop_times.cache()
    filtered_stop_times.repartition(partitions, 'trip_id').write.parquet(f'{output_directory}/{day}/stop_times', mode='overwrite')
    return filtered_stop_times

def get_filtered_stops():
    filtered_stops = spark.sql(f"SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops WHERE stop_id IN (SELECT stop_id FROM filtered_stop_times)")
    filtered_stops = filtered_stops.cache()
    return filtered_stops


# Reference: https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def calculate_distance(from_lat, from_lon, to_lat, to_lon):
    dlon = radians(to_lon - from_lon)
    dlat = radians(to_lat - from_lat)
    to_lat = radians(to_lat)
    from_lat = radians(from_lat)
    a = sin(dlat / 2)**2 + cos(from_lat) * cos(to_lat) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6372.8
    return c * r * 1000


def get_filtered_transfers(day, filtered_stops):
    udf_calculate_distance = functions.udf(calculate_distance, types.FloatType())
    walking_speed = 1.4 # 1.4 m/s = 5 km/h
    join_stops = filtered_stops.withColumnRenamed('stop_id', 'to_stop_id').withColumnRenamed('stop_lat', 'to_stop_lat').withColumnRenamed('stop_lon', 'to_stop_lon')
    transfers = filtered_stops.crossJoin(join_stops)
    transfers = transfers.withColumn('distance', udf_calculate_distance(transfers['stop_lat'], transfers['stop_lon'], transfers['to_stop_lat'], transfers['to_stop_lon']))
    transfers = transfers.filter(transfers.stop_id != transfers.to_stop_id).filter(transfers['distance'] < 100)
    transfers = transfers.withColumn('transfer_time', transfers['distance'] / walking_speed)
    transfers = transfers.select('stop_id', 'to_stop_id', 'transfer_time')
    transfers = transfers.withColumnRenamed('stop_id', 'from_stop_id')
    transfers.sort('from_stop_id').write.parquet(f'{output_directory}/{day}/transfers', mode='overwrite')
    filtered_stops.select('stop_id', 'stop_name').sort('stop_id').write.parquet(f'{output_directory}/{day}/stops', mode='overwrite')

def read_files():
    calendar = spark.read.csv(f'{input_directory}/calendar.txt', schema=define_calendar_schema(), header=True, sep=",")
    trips = spark.read.csv(f'{input_directory}/trips.txt', schema=define_trips_schema(), header=True, sep=",")
    stop_times = spark.read.csv(f'{input_directory}/stop_times.txt', schema=define_stop_times_schema(), header=True, sep=",")
    stops = spark.read.csv(f'{input_directory}/stops.txt', schema=define_stops_schema(), header=True, sep=",")
    return calendar, trips, stop_times, stops
    

def repartition_dataframes(calendar, trips, stop_times, stops, partitions):
    calendar = calendar.repartition(partitions, 'service_id')
    trips = trips.repartition(partitions, 'route_id')
    stop_times = stop_times.repartition(partitions, 'trip_id')
    stops = stops.repartition(partitions, 'stop_id')
    return calendar, trips, stop_times, stops

def main(partitions = 1):
    calendar, trips, stop_times, stops = read_files()
    calendar, trips, stop_times, stops = repartition_dataframes(calendar, trips, stop_times, stops, partitions)
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    # Filter out trips, routes, stop_times, stops that are not in service on the given day
    print("\n----------------------------------------------")
    for day in days:
        # Create temporary views
        create_views(calendar, trips, stop_times, stops)
        # Get all service_ids that are in service on the given day
        df = spark.sql(f"SELECT * FROM calendar WHERE {day} = 1")
        df.createOrReplaceTempView('calendar')

        print(f"Getting trips that are in service on {day.title()}")
        filtered_trips = get_filtered_trips(day)

        print(f"Getting stop times that are in service on {day.title()}")
        get_filtered_stop_times(day, filtered_trips, partitions)

        print(f"Getting stops that are in service on {day.title()}")
        filtered_stops = get_filtered_stops(day)

        print(f"Getting transfers based on distance that are in service on {day.title()}")
        get_filtered_transfers(day, filtered_stops)
        print(f"Pre-Processing completed for {day.title()}")
        print("----------------------------------------------\n")

if __name__ == '__main__':
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    partitions = 1
    if(len(sys.argv) > 3):
        partitions = int(sys.argv[3]) # Number of nodes in the cluster
    spark = SparkSession.builder.appName('Preprocessing GTFS').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(partitions)
