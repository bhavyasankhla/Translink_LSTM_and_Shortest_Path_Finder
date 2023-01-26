import time
import subprocess

while (True):
    subprocess.call("python3 load_gtfs_realtime_to_azure.py", shell=True)
    time.sleep(300)
