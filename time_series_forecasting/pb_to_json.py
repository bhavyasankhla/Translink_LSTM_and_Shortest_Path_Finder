# Required imports
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
from pathlib import Path
import os
import sys

def convert(input_dir):
    Path(f"{input_dir}_json").mkdir(parents=True, exist_ok=True)
    for file in os.listdir(input_dir):
        filename = os.path.join(input_dir, file) 
        saved_feed = open(filename, 'rb').read()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(saved_feed)
        feedJSON = MessageToJson(feed)
        stem = Path(file).stem
        f = open(f"{input_dir}_json/{stem}.json", 'w')
        f.write(feedJSON)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    convert(input_dir)
