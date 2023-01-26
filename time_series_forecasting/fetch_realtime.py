import time
import urllib.request

def fetch_realtime_data(timestamp):
    urllib.request.urlretrieve("https://gtfs.translink.ca/v2/gtfsrealtime?apikey=NXdv94cxvQsH9z9b7vY3", f"realtime_data/realtime-{round(timestamp)}.pb")
    urllib.request.urlretrieve("https://gtfs.translink.ca/v2/gtfsposition?apikey=NXdv94cxvQsH9z9b7vY3", f"realtime_position_data/realtime-position-{round(timestamp)}.pb")
    urllib.request.urlretrieve("https://gtfs.translink.ca/v2/gtfsrealtime?apikey=NXdv94cxvQsH9z9b7vY3", f"realtime_alerts_data/realtime-alerts-{round(timestamp)}.pb")


starttime = time.time()
count = 1
print("Fetching new data...")
while True:
    fetch_realtime_data(time.time())
    print(f"Iteration #{count}: Fetched new data")
    time.sleep(30 - ((time.time() - starttime) % 30))
    print("Fetching new data...")
    count += 1
