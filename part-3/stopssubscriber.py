from google.cloud import storage
import gzip
import zlib
import io
import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import psycopg2
import pandas as pd
import datetime
import threading
import time
DBname = "postgres"
DBuser = "postgres"
DBpwd = "Project@123"


def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

def createTable(conn):
    with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS STOP (
                    trip_id INT PRIMARY KEY,
                    route_id INT,
                    vehicle_id INT,
                    service_key VARCHAR(10),
                    direction INT
                );
            """)

def loadStopTable(conn,df):
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(f"""
                INSERT INTO STOP VALUES (
                '{row.trip_id}',
                '{row.route_id}',
                '{row.vehicle_id}',
                '{row.service_key}',
                '{row.direction}'
                );
            """)

def loadTripView(conn):
    with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE OR REPLACE VIEW TRIP_VW AS (
                SELECT t1.trip_id,t2.route_id,t1.vehicle_id,t2.service_key,t2.direction
                FROM TRIP t1
                JOIN STOP t2 ON t1.trip_id=t2.trip_id AND t1.vehicle_id = t2.vehicle_id
                );
            """)
last_message_time = time.time()

def getDecodedDate(date):
    dt = datetime.datetime.strptime(date, "%d%b%Y:%H:%M:%S")
    return dt

def getTimeStamp(time):
    ts = datetime.timedelta(seconds=time)
    return ts

def message_received():
    global last_message_time
    last_message_time = time.time()

bucket_name = "archiivebucket"
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

def upload_to_gcs(data, bucket_name, file_name):
    """Uploads data to the specified GCS bucket."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type="application/octet-stream")
    print(f"File {file_name} uploaded to {bucket_name}.")

if __name__ == '__main__':
    # Initialize Pub/Sub client and subscription path
    project_id = "dataeng-saheli-bavirisetty"
    subscription_id = "proj-stops-sub"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Connect to PostgreSQL database
    conn = dbconnect()
    print("Database connected")

    # Create tables if they don't exist
    createTable(conn)
    #print("Tables TRIP and BREADCRUMB created")

    # Initialize variables for storing messages
    messages = []

    # Define callback function to process Pub/Sub messages
    def callback(message):
        event = json.loads(message.data.decode('utf-8'))
        messages.append(event)
        message.ack()
        message_received()

    # Subscribe to Pub/Sub topic
    subscriber.subscribe(subscription_path, callback=callback)
    print("Subscribed to Pub/Sub topic")

    # Define a thread to check for inactivity and stop listening if no messages received
    def check_activity():
        global last_message_time
        while True:
            if time.time() - last_message_time > 60:  # Adjust the timeout duration as needed
                subscriber.close()
                print("No messages received for 60 seconds. Exiting...")
                break
            time.sleep(1)  # Check every second for activity

    activity_thread = threading.Thread(target=check_activity)
    activity_thread.start()

    # Keep the main thread running until the activity thread stops
    try:
        activity_thread.join()
    except KeyboardInterrupt:
        pass

    print("\nTotal Records: " + str(len(messages)))

    if messages:
        # Create a file name with a timestamp
        file_name = f"project_stops_compressed_{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.json.gz"
        # Convert all data to JSON string
        data_str = json.dumps(messages)
        # Compress the JSON string using zlib
        compressed_data = zlib.compress(data_str.encode('utf-8'))
        # Upload compressed data to GCS
        upload_to_gcs(compressed_data, bucket_name, file_name)

    df = pd.DataFrame(messages)


    loadStopTable(conn,df)
    print("Data loaded to table stop")
    loadTripView(conn)
    print("View trip_vw updated")
    conn.close()
    print("Database connection closed")
