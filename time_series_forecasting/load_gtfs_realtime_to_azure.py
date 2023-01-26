import psycopg2
import datetime
import logging
from google.transit import gtfs_realtime_pb2
import protobuf_json
import urllib.request

# Update connection string information
host = "db732.postgres.database.azure.com"
dbname = "postgres"
user = "admin732@db732"
password = "Datainferno4$"
sslmode = "require"

# Gets or creates a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('logfile_gtfs_data_load.log')

# add file handler to logger
logger.addHandler(file_handler)

# Construct connection string
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
conn = psycopg2.connect(conn_string)

current_time = datetime.datetime.now()
time_stamp_current = current_time.timestamp()
current_time = datetime.datetime.fromtimestamp(time_stamp_current)

logger.info('...')
logger.info("Connection to azure postgres DB established at : " + str(current_time))

cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS trips ("
               "trip_id VARCHAR(50), "
               "start_date VARCHAR(50), "
               "route_id VARCHAR(50), "
               "direction_id INTEGER,"
               "timestamp INTEGER, "
               "PRIMARY KEY(trip_id, timestamp));")

cursor.execute("CREATE TABLE IF NOT EXISTS stops ("
               "stop_id VARCHAR(50), "
               "trip_id VARCHAR(50), "               
               "stop_sequence INTEGER, "
               "arrival_delay INTEGER, "
               "arrival_time INTEGER,"
               "departure_delay INTEGER,"
               "departure_time INTEGER,"
               "timestamp INTEGER, "
               "PRIMARY KEY(stop_id, trip_id, stop_sequence, timestamp));")

cursor.execute("CREATE TABLE IF NOT EXISTS vehicles ("
               "vehicle_id VARCHAR(50) , "
               "trip_id VARCHAR(50), "
               "vehicle_label VARCHAR(50),"
               "timestamp INTEGER, "
               "PRIMARY KEY (vehicle_id, trip_id, timestamp));")

gtfs_realtime = gtfs_realtime_pb2.FeedMessage()
gtfs_realtime.ParseFromString(urllib.request.urlopen('https://gtfs.translink.ca/v2/gtfsrealtime?apikey=M7xMnphu0DwuUeZ2YiYg').read())
content = protobuf_json.pb2json(gtfs_realtime)
logger.info("Received realtime data")

trips_count = 0
stops_count = 0
vehicles_count = 0
trip_data = []
stops_data = []
vehicles_data = []

for each_entity in content['entity']:
    trips_count += 1
    entity_id = each_entity['id']
    trip_update = each_entity['trip_update']
    timestamp = -1
    if trip_update.get('timestamp'):
        timestamp = trip_update['timestamp']

    trip = trip_update['trip']
    trip_id = trip['trip_id']
    start_date = trip['start_date']
    route_id = trip['route_id']
    direction_id = trip['direction_id']

    db_row = (trip_id, start_date, route_id, direction_id, timestamp)
    trip_data.append(db_row)
    stop_time_update = trip_update['stop_time_update']

    for each_stop_time_update in stop_time_update:
        if each_stop_time_update:
            stop_sequence = each_stop_time_update['stop_sequence']
            stop_id = each_stop_time_update['stop_id']
            arrival_delay = -1
            arrival_time = -1
            if each_stop_time_update.get('arrival'):
                arrival_delay = each_stop_time_update['arrival']['delay']
                arrival_time = each_stop_time_update['arrival']['time']
            departure_delay = -1
            departure_time = -1
            if each_stop_time_update.get('departure'):
                departure_delay = each_stop_time_update['departure']['delay']
                departure_time = each_stop_time_update['departure']['time']
            stops_count += 1
            stops_data.append((stop_id, trip_id, stop_sequence, arrival_delay, arrival_time, departure_delay, departure_time, timestamp))

    vehicle = trip_update['vehicle']
    if vehicle:
        vehicle_id = vehicle['id']
        vehicle_label = vehicle['label']
        vehicles_data.append((vehicle_id, trip_id, vehicle_label, timestamp))
        vehicles_count += 1

logger.info(str(stops_count) + ' stops rows')
logger.info(str(trips_count) + ' trips rows')
logger.info(str(vehicles_count) + ' vehicles rows')
logger.info("timestamp: " + str(timestamp))

## INSERTING TRIPS DATA
trips_args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in trip_data)
cursor.execute("INSERT INTO "
               "trips (trip_id, start_date, route_id, direction_id, timestamp) "
               "VALUES " + trips_args_str +
               " ON CONFLICT (trip_id, timestamp) DO UPDATE SET "
               "(start_date, route_id, direction_id) = "
               "(EXCLUDED.start_date, EXCLUDED.route_id, EXCLUDED.direction_id);")

## INSERTING STOPS DATA
stops_args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s, %s)", x).decode('utf-8') for x in stops_data)
cursor.execute("INSERT INTO "
               "stops (stop_id, trip_id, stop_sequence, arrival_delay,arrival_time, "
               "departure_delay, departure_time, timestamp) "
               "VALUES " + stops_args_str + " ON CONFLICT (stop_id, trip_id, stop_sequence, timestamp) "
               "DO UPDATE SET (arrival_delay, arrival_time, departure_delay, departure_time) = "
               "(EXCLUDED.arrival_delay, EXCLUDED.arrival_time, EXCLUDED.departure_delay, EXCLUDED.departure_time);")

## INSERTING VEHICLES DATA
vehicles_args_str = ','.join(cursor.mogrify("(%s,%s,%s, %s)", x).decode('utf-8') for x in vehicles_data)
cursor.execute("INSERT INTO "
               "vehicles (vehicle_id, trip_id, vehicle_label, timestamp)"
               "VALUES " + vehicles_args_str +
               " ON CONFLICT (vehicle_id, trip_id, timestamp) DO UPDATE SET vehicle_label = "
               "EXCLUDED.vehicle_label")

current_time = datetime.datetime.now()
time_stamp_current = current_time.timestamp()
current_time = datetime.datetime.fromtimestamp(time_stamp_current)

logger.info("Data insert complete at : " + str(current_time))
logger.info('...')
# Clean up
conn.commit()
cursor.close()
conn.close()
