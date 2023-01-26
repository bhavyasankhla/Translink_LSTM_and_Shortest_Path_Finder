from datetime import datetime, timedelta
from pyspark.sql import SparkSession, types, functions
import sys

    
    
def main(input,output):
#Code to read trip file. 
   
    trips=spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true").csv('trips.txt') 
#Getting trip Id for a particular Route and a direction related to that route. Each route will have 2 dir. For ex. SFU-Metrotown will be one direction and Metrotown to SFU another.

    trip_ids=trips.filter((trips['route_id']==input) & (trips['direction_id']==1)).select(trips['trip_id']).collect()
    trip_ids_list=tuple(map(lambda n: str(n[0]),trip_ids))
#Connecting to postgreSql and getting records from the table based on the trip id collected before it.
    df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://db732.postgres.database.azure.com/postgres") \
    .option("dbtable", "(select * from stops where trip_id in {}  ORDER by arrival_delay ASC limit 10000 ) as t".format(trip_ids_list)) \
    .option("user", "admin732@db732") \
    .option("password", "Datainferno4$") \
    .option("driver", "org.postgresql.Driver") \
    .load()
    df.show()
#Code to read  stop, stop_times, calendar and calendar_excep files.

    stops=spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true").csv('stops.txt')
    stop_times=spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true").csv('stop_times.txt')
    calendar=spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true").csv('calendar.txt')    
    calendar_excep=spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true").csv('calendar_dates.txt')
    
# Code to join Trip, stop and Stop time dataframes to get stops for each trip and their scheduled time.

    distinctStartEndDate=calendar.select(functions.min("start_date"),functions.max("end_date")).collect()
    startDate= datetime.strptime(str(distinctStartEndDate[0][0]),'%Y%m%d')
    endDate=datetime.strptime( str(distinctStartEndDate[0][1]),"%Y%m%d")
    delta = endDate - startDate  # as timedelta
    days = [[(startDate + timedelta(days=i)).strftime("%Y%m%d")] for i in range(delta.days + 1)]
    daysDF=spark.createDataFrame(data=days,schema=["sch_date"])
    tripStop=trips.join(stop_times,"trip_id")
    tripStopName=tripStop.join(stops,"stop_id").select('trip_id',"route_id","service_id","arrival_time","departure_time","stop_id","stop_sequence","stop_name","stop_lat","stop_lon").withColumnRenamed('arrival_time','sch_arrival_time').withColumnRenamed('departure_time','sch_departure_time')
    print(tripStopName.select('stop_sequence').distinct().show())
    
# Code to join static dataframe with real time dataframe.

    joinedData=tripStopName.join(df,['stop_id','trip_id']).drop(df['stop_sequence']).drop(df["timestamp"]).drop(tripStopName['service_id']).withColumn('sch_arrival_time',functions.col('arrival_time')-functions.col("arrival_delay")) \
        .withColumn('sch_departure_time',functions.col('departure_time')-functions.col("departure_delay")) \
        .withColumn('arrival_time',functions.col('departure_time')) \
        .withColumn('departure_time',functions.col('departure_time'))
        
# Logic to remove duplicate records and only keeping one with Max Delay.

    joinedData=joinedData.withColumn('trip_date',functions.from_unixtime(functions.col('arrival_time'),"MM-dd-yyyy")).cache()
    groupedjoinedData=joinedData.groupBy('trip_id','stop_id','trip_date').max('arrival_delay')
    groupedjoinedData=groupedjoinedData.withColumnRenamed('max(arrival_delay)','arrival_delay')
    joinedData=groupedjoinedData.join(joinedData,['trip_id','stop_id','trip_date','arrival_delay'])
    joinedData.coalesce(1).write.csv(output,header=True, mode='overwrite')
    joinedData.show()
    print(joinedData.count())
    
    
    
    
    

if __name__ == '__main__':
    spark=SparkSession.builder.config('spark.jars','/home/hduser/Project/postgresql-42.4.0.jar').appName('Fuse Table').getOrCreate()
    sc = spark.sparkContext
    output = sys.argv[2]
    input= sys.argv[1]
    configurations = spark.sparkContext.getConf().getAll()
    for item in configurations: 
        print(item)
    main(input,output)
    
    
    
    