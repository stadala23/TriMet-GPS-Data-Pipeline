from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen
from urllib.error import HTTPError
import pandas as pd
import re
import os
import json
from google.cloud import pubsub_v1

# Set environment variables
os.environ['PATH'] = '/usr/local/bin:/usr/bin:/bin'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/saheli/dataeng-saheli-bavirisetty-533e8168c7d6.json'

# Pub/Sub details
project_id = "dataeng-saheli-bavirisetty"
topic_id = "proj-stops-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


# List of vehicle IDs
vehicle_ids = [
    3028, 3914, 3963, 4227, 3952, 3701, 3519, 3403, 3252, 3639,
    4041, 3251, 3045, 4011, 3613, 4531, 3246, 3245, 3132, 3569,
    3722, 3955, 3915, 3556, 3621, 3169, 3226, 3551, 3007, 3406,
    4220, 3322, 2916, 3025, 3416, 3650, 2915, 4052, 3745, 3725,
    3035, 3150, 4035, 99222, 3206, 3156, 3529, 3321, 3512, 4034,
    3959, 3504, 3564, 3927, 3005, 4065, 4236, 3502, 3264, 3124,
    3801, 4516, 3749, 3562, 3170, 4042, 3106, 4066, 4235, 4033,
    3326, 3118, 3605, 3724, 3015, 3136, 4530, 3209, 4001, 3010,
    3527, 3262, 3229, 3704, 2906, 2930, 3057, 3054, 3247, 3325,
    3645, 4056, 3922, 3040, 3716, 4028, 4020, 2910, 3058, 3720
]

for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
    try:
        html = urlopen(url)
    except HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason}")
        continue  # Skip to the next iteration if there's an HTTP error

    soup = BeautifulSoup(html, 'html.parser')

    dataframes = []
    trip_id = []
    tables = soup.find_all('table')
    tags = soup.find_all('h2')

    trip_date_string = soup.find_all('h1')[0].text.split()[-1]
    trip_date = datetime.strptime(trip_date_string, "%Y-%m-%d")
    day_name = trip_date.strftime('%A')

    # Extract trip IDs
    for tag in tags:
        text = tag.get_text(strip=True)
        stripped_text = re.findall(r'\d+', text)
        trip_id.append(stripped_text)

    # Process tables and assign trip IDs
    for i, table in enumerate(tables):
        df = pd.read_html(str(table))[0]
        required_columns = ['vehicle_number', 'service_key', 'route_number', 'direction']
        df = df[required_columns]
        trip_ids = [x[0] for x in [trip_id[i % len(trip_id)]]]* len(df)
        trip_ids = [x.strip('[]') for x in trip_ids]
        df['trip_id'] = trip_ids
        dataframes.append(df)

    final_df = pd.concat(dataframes, ignore_index=True)
    final_df['service_key'] = day_name
    final_df.loc[~final_df['service_key'].isin(['Saturday', 'Sunday']), 'service_key'] = 'Weekday'

    final_df = final_df.drop_duplicates().reset_index(drop=True)
    column_names_new = {'vehicle_number': 'vehicle_id', 'route_number': 'route_id'}
    final_df = final_df.rename(columns=column_names_new).reindex(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
    count = 0
    for _, row in final_df.iterrows():
        count+= 1
        data_json = row.to_json()
        future = publisher.publish(topic_path, data_json.encode('utf-8'))
       # print(f"Published data for vehicle {vehicle_id}")

print(f"Total records {count}")
print("Data processed and published successfully.")

