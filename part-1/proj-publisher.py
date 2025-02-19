import requests
from google.cloud import pubsub_v1
import json
import os
from datetime import datetime

# Set environment variables
os.environ['PATH'] = '/usr/local/bin:/usr/bin:/bin'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/saheli/dataeng-saheli-bavirisetty-533e8168c7d6.json'

# Pub/Sub details
project_id = "dataeng-saheli-bavirisetty"
topic_id = "proj-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

vehicle_ids = [
    3028,3914,3963,4227,3952,3701,3519,3403,3252,3639,
    4041,3251,3045,4011,3613,4531,3246,3245,3132,3569,
    3722,3955,3915,3556,3621,3169,3226,3551,3007,3406,
    4220,3322,2916,3025,3416,3650,2915,4052,3745,3725,
    3035,3150,4035,99222,3206,3156,3529,3321,3512,4034,
    3959,3504,3564,3927,3005,4065,4236,3502,3264,3124,
    3801,4516,3749,3562,3170,4042,3106,4066,4235,4033,
    3326,3118,3605,3724,3015,3136,4530,3209,4001,3010,
    3527,3262,3229,3704,2906,2930,3057,3054,3247,3325,
    3645,4056,3922,3040,3716,4028,4020,2910,3058,3720,
]

for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        # Publish each record to Pub/Sub
        for record in data:
            data_str = json.dumps(record)
            data_bytes = data_str.encode("utf-8")
            future = publisher.publish(topic_path, data_bytes)
            #print(f"Published message {future.result()} to {topic_path}")

        print(f"Successfully published data for vehicle ID: {vehicle_id}")

    except requests.RequestException as e:
        print(f"An error occurred for vehicle ID {vehicle_id}: {e}")

print("Data publishing process completed.")

