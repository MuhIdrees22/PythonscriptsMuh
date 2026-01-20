import os
import glob
import json
from google.cloud import pubsub_v1

# 1) Auth: use the first JSON file in the current folder
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No .json key file found in this folder. Copy your service account key here.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# 2) Set your project + subscription
project_id = "pure-wall-451118-g4"
subscription_id = "labels-sub"  # <-- change to your subscription name

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data_str = message.data.decode("utf-8")
        row_dict = json.loads(data_str)

        # Print the dictionary (or specific fields)
        print("Received row:", row_dict)

        message.ack()
    except Exception as e:
        print("Error processing message:", e)
        # Nack so Pub/Sub can redeliver (useful for debugging)
        message.nack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped consumer.")
