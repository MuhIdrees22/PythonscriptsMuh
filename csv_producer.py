import os
import glob
import csv
import json
from google.cloud import pubsub_v1

# 1) Auth: use the first JSON file in the current folder
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No .json key file found in this folder. Copy your service account key here.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# 2) Set your project + topic
project_id = "pure-wall-451118-g4"
topic_name = "labels"  

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing CSV rows to {topic_path}")

# 3) Read CSV and publish each row
csv_file = "Labels.csv"  # <-- filename

with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)  # each row becomes a dict automatically
    count = 0

    for row in reader:
        # Convert dict -> JSON string -> bytes
        message_str = json.dumps(row)
        message_bytes = message_str.encode("utf-8")

        future = publisher.publish(topic_path, message_bytes)
        msg_id = future.result()

        count += 1
        print(f"Published row {count} (message_id={msg_id}): {row}")

print(f"Done. Published {count} rows.")

