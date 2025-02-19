import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project_id = "dataeng-saheli-bavirisetty"
subscription_id = "proj-topic-sub"

today = datetime.now().strftime('%Y-%m-%d')

file_name = f"/home/saheli/subscrided_vehicle_data_{today}.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

records_consumed = 0

def callback(message):
    global records_consumed

    if message.data:
        try:
            data = json.loads(message.data.decode("utf-8"))
            records_consumed += 1

            with open(file_name, 'a') as file:
                json.dump(data, file, indent=4)
                file.write('\n')

        except json.JSONDecodeError:
            print("Error decoding JSON data from message.")

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

print(f"Total records consumed: {records_consumed}")
