#!/usr/bin/env python
from google.cloud import storage
import gzip
import zlib
import io
import psycopg2
import pandas as pd
import datetime
import json
import math
import threading
import time
from google.cloud import pubsub_v1

DBname = "postgres"
DBuser = "postgres"
DBpwd = "Project@123"
TableName = "XYZ"

def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

# Define a global variable to track message receipt time
last_message_time = time.time()

def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TRIP (
                trip_id INT PRIMARY KEY,
                route_id INT,
                vehicle_id INT,
                service_key VARCHAR(10),
                direction INT
            )
        """)
        print("TRIP table created.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS BREADCRUMB (
                tstamp TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                speed FLOAT,
                trip_id INTEGER REFERENCES TRIP(trip_id)
            )
        """)
        print("BREADCRUMB table created.")



def getDecodedDate(date):
    dt = datetime.datetime.strptime(date, "%d%b%Y:%H:%M:%S")
    return dt

def getTimeStamp(time):
    ts = datetime.timedelta(seconds=time)
    return ts

def loadTripTable(conn, unique_combinations_df):
    with conn.cursor() as cursor:
        for row in unique_combinations_df.itertuples():
            cursor.execute("""
                INSERT INTO TRIP (trip_id, route_id, vehicle_id, service_key, direction)
                VALUES (%s, %s, %s, %s, %s)
            """, (row.EVENT_NO_TRIP, 0, row.VEHICLE_ID, row.SERVICE_KEY, 0))

def loadBreadCrumbTable(conn, df):
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("""
                INSERT INTO BREADCRUMB(tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (row.DATETIMESTAMP, row.GPS_LATITUDE, row.GPS_LONGITUDE, row.SPEED, row.EVENT_NO_TRIP))

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
    subscription_id = "proj-topic-sub"
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

    # Save all received data to a GCS bucket
    if messages:
        # Create a file name with a timestamp
        file_name = f"project_compressed_{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.json.gz"
        # Convert all data to JSON string
        data_str = json.dumps(messages)
        # Compress the JSON string using zlib
        compressed_data = zlib.compress(data_str.encode('utf-8'))
        # Upload compressed data to GCS
        upload_to_gcs(compressed_data, bucket_name, file_name)


    df = pd.DataFrame(messages)
    
    # Data transformation and loading into PostgreSQL tables
    # Your data transformation and loading code here...

    # Perform data transformation
    df2 = pd.DataFrame(data=df, columns=['OPD_DATE'])
    df2 = df2.applymap(getDecodedDate)
    df2 = df2.rename(columns={'OPD_DATE': 'DATE'})
    df3 = pd.DataFrame(data=df, columns=['ACT_TIME'])
    df3 = df3.applymap(getTimeStamp)
    df3 = df3.rename(columns={'ACT_TIME': 'TIME'})
    df4 = pd.concat([df2, df3], axis=1)
    df4 = df4.assign(DATETIMESTAMP=df4['DATE'] + df4['TIME'])
    df['DATETIMESTAMP'] = pd.Series(df4['DATETIMESTAMP'])
    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dDATETIMESTAMP'] = df.groupby('EVENT_NO_TRIP')['DATETIMESTAMP'].diff()
    calculateSpeed = lambda x: round(x['dMETERS'] / x['dDATETIMESTAMP'].total_seconds(), 2) if not math.isnan(
        x['dMETERS']) and not math.isnan(x['dDATETIMESTAMP'].total_seconds()) else None
    df['SPEED'] = df.apply(calculateSpeed, axis=1)
    df['SPEED'] = df['SPEED'].fillna(0)
    df = df.drop(columns=['dMETERS', 'dDATETIMESTAMP'])
    df['SERVICE_KEY'] = df['DATETIMESTAMP'].dt.day_name
    df.loc[~df['SERVICE_KEY'].isin(['Saturday', 'Sunday']), 'SERVICE_KEY'] = 'Weekday'
    pd.set_option('display.float_format', '{:.2f}'.format)


def validate_data(df):

    print("\nValidating data...")
    print("\nAssertion 1: Check that all speed values are non-negative and reasonable")
    if (df['SPEED'] < 0).any():
        print("Negative speed values found!")
    else:
        print("All speed values are positive.")

    print("\nAssertion 2: Ensure SPEED column is in the correct format")
    if df['SPEED'].dtype != 'float64':
        df['SPEED'] = pd.to_numeric(df['SPEED'], errors='coerce')
        print("Converted 'SPEED' column to float format.")
    else:
        print("SPEED values are confirmed to be in float format.")

    print("\nAssertion 3: For 'EVENT_NO_TRIP' format: 9 digits long and starts with '2'. ")
    if df['EVENT_NO_TRIP'].apply(lambda x: isinstance(x, int) and len(str(x)) == 9 and str(x).startswith('2')).all():
        print("All trip_id values are 9 digits long and start with digit '2'.")
    else:
        print("Error: Some trip_id values do not conform to the 9 digits requirement or do not start with '2'.")

    print("\nAssertion 4: For 'VEHICLE_ID' format: must be an int and exactly 4 digits")
    if df['VEHICLE_ID'].apply(lambda x: isinstance(x, int) and 1000 <= x <= 9999).all():
        print("All vehicle_id values are integers and exactly 4 digits.")
    else:
        print("Error: Some vehicle_id values are not integers or not exactly 4 digits.")


    print("\nAssertion 5: Existence Check for Essential Columns")
    essential_columns = ['EVENT_NO_TRIP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED']
    missing_columns = [col for col in essential_columns if col not in df.columns]
    if missing_columns:
        print(f"Missing essential columns: {missing_columns}")
    else:
        print("All essential columns are present.")

    print("\nAssertion 6: Valid GPS Coordinate Check Intra-record Check")

    if ((df['GPS_LATITUDE'] < -90) | (df['GPS_LATITUDE'] > 90)).any() or ((df['GPS_LONGITUDE'] < -180) | (df['GPS_LONGITUDE'] > 180)).any():
        print("Out-of-range GPS coordinates found!")
    else:
        print("All GPS coordinates are within valid ranges.")
    print("\nAssertion 7:  Distribution Check")

    if df['SPEED'].skew() > 2:
        print("Speed distribution is highly skewed.")
    else:
        print("Speed distribution appears normal.")

    print("\nAssertion 8: tstamp is a string and in the format DD MMM YYYY:HH:MM:SS")
    df['DATETIMESTAMP'] = pd.to_datetime(df['DATETIMESTAMP'], errors='coerce')
    if df['DATETIMESTAMP'].isnull().any():
        print("Data in 'DATETIMESTAMP' column is not in the same format.")
    else:
        print("Data in 'DATETIMESTAMP' column is in the same format.")

    print("\nAssertion 9: Each event_no_trip should have only one unique vehicle_id. ")

    unique_vehicle_count = df.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].nunique()
    event_with_diff_vehicle = unique_vehicle_count[unique_vehicle_count > 1]
    if not event_with_diff_vehicle.empty:
        print("Events with different vehicle_id values:")
        print(event_with_diff_vehicle)
    else:
        print("Each event_no_trip have only one unique vehicle_id.")

    print("\nAssertion 10: Meters covered by any vehicle cannot be negative.")
    negative_meters = df[df['METERS'] < 0]
    if not negative_meters.empty:
        print("Vehicles with negative meters covered:")
        print(negative_meters)
    else:
        print("All vehicles have non-negative meters covered.")

    print("\nAssertion 11: There cannot exist a trip without a vehicle id.")
    trips_without_vehicle_id = df[df['VEHICLE_ID'].isna()]
    if not trips_without_vehicle_id.empty:
        print("Trips without a vehicle_id:")
        print(trips_without_vehicle_id)
    else:
        print("All trips have a vehicle_id.")

    print("\nValidation complete. Proceed with data processing.")

    return df

validate_data(df)

#Load data into PostgreSQL tables
print("\nLoading data to table TRIP...")
loadTripTable(conn, df[['EVENT_NO_TRIP', 'VEHICLE_ID', 'SERVICE_KEY']].drop_duplicates())
print("Data loaded to table TRIP")
print("\nLoading data to table BREADCRUMB...")
loadBreadCrumbTable(conn, df)
print("Data loaded to table BREADCRUMB")


# Close database connection
conn.close()
print("Database connection closed")


