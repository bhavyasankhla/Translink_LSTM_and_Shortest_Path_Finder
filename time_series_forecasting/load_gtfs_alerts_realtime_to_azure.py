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
file_handler = logging.FileHandler('logfile_gtfs_alerts_data_load.log')

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

cursor.execute("CREATE TABLE IF NOT EXISTS alerts ("
               "alert_id VARCHAR(50), "
               "route_id VARCHAR(50), "
               "active_period_start INTEGER, "
               "active_period_end INTEGER, "
               "stop_id VARCHAR(50), "
               "route_type INTEGER,"
               "timestamp INTEGER, "
               "cause INTEGER, "
               "effect INTEGER, "
               "severity_level INTEGER, "
               "PRIMARY KEY(alert_id, stop_id, route_id));")

gtfs_realtime = gtfs_realtime_pb2.FeedMessage()
gtfs_realtime.ParseFromString(urllib.request.urlopen('https://gtfs.translink.ca/v2/gtfsalerts?apikey=hn5hpwc7HX3bktazhYAY').read())
content = protobuf_json.pb2json(gtfs_realtime)
logger.info("Received realtime data")

alert_count = 0
informed_stop_route_combo = 0
alerts_data = []

timestamp = -1
if content['header'].get('timestamp'):
    timestamp = content['header']['timestamp']

for each_entity in content['entity']:

    alert_count += 1
    alert_id = each_entity['id']
    alert = each_entity['alert']

    active_period = alert['active_period']
    active_period_start = active_period[0]["start"]
    active_period_end = -1
    if active_period[0].get('end'):
        active_period_end = active_period[0]['end']

    cause = alert['cause']
    effect = alert['effect']
    severity_level = alert['severity_level']

    for each_informed_entity in alert['informed_entity']:
        informed_stop_route_combo += 1
        route_type = each_informed_entity['route_type']
        if each_informed_entity.get('route_id'):
            route_id = each_informed_entity['route_id']
            stop_id = -1
            if each_informed_entity.get('stop_id'):
                stop_id = each_informed_entity['stop_id']
            alerts_data.append((alert_id, route_id, active_period_start, active_period_end, stop_id, route_type, timestamp
                                , cause, effect, severity_level))

logger.info(str(alert_count) + ' alert rows')
logger.info(str(informed_stop_route_combo) + ' informed_stop_route_combo rows')
logger.info("timestamp: " + str(timestamp))

#
## INSERTING ALERTS DATA
alerts_args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in alerts_data)
cursor.execute("INSERT INTO "
               "alerts (alert_id, route_id, active_period_start, active_period_end, stop_id, route_type, timestamp, "
               "cause, effect, severity_level) "
               "VALUES " + alerts_args_str + " ON CONFLICT (alert_id, stop_id, route_id) "
               "DO UPDATE SET (active_period_start, active_period_end, route_type, timestamp, cause, effect, "
               "severity_level) = "
               "(EXCLUDED.active_period_start, EXCLUDED.active_period_end, EXCLUDED.route_type, EXCLUDED.timestamp, "
               "EXCLUDED.cause, EXCLUDED.effect, EXCLUDED.severity_level);")

current_time = datetime.datetime.now()
time_stamp_current = current_time.timestamp()
current_time = datetime.datetime.fromtimestamp(time_stamp_current)

logger.info("Data insert complete at : " + str(current_time))
logger.info('...')
# Clean up
conn.commit()
cursor.close()
conn.close()
