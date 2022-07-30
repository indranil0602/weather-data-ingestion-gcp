import os
import json
import base64
import requests
from datetime import datetime
from pytz import timezone 

# GCP imports

from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import pubsub_v1

TIME_ZONE = "Asia/Kolkata"

time_stamp = datetime.now(timezone(TIME_ZONE)).strftime("%Y%m%d-%H:%M:%S")
current_date = datetime.now(timezone(TIME_ZONE)).strftime("%Y-%m-%d")
current_time = datetime.now(timezone(TIME_ZONE)).strftime("%H:%M:%S")
current_year = datetime.now(timezone(TIME_ZONE)).strftime("%Y")

f = open("config.json", "r")
data = json.load(f)

PROJECT_ID = data.get("project_id")
BASE_URL = data.get("BASE_URL")

PUBSUB_LOGGING_TOPIC = 'hourly-weather-data-load-logging-topic'
PUBSUB_LOGGING_SERVICE = 'Cloud Function'

# defining message data for the pubsub topic

MESSAGE_DATA = {
    "project":PROJECT_ID,
    "service":PUBSUB_LOGGING_SERVICE,
    "process":"Call Openweather Current Weather Data Api",
    "runtime":time_stamp,
    "log_type":"api-logging"
}

def get_api_key():
    """ Get the API key from secrate manager GCP and return the API key"""

    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = data.get("secret_name")
    request = {"name" : f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"}
    response = secret_client.access_secret_version(request)
    API_KEY = response.payload.data.decode("UTF-8") 
    return API_KEY  

def check_list_of_buckets(bucket_name):
    """ Get the list of all buckets from GCP and find if the desired bucket is present or not"""

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket_name == bucket.name:
            return True

    return False 

def create_new_bucket(bucket_name):
    """ Creating new GCS bucket and return the bucket instance"""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="ASIA-SOUTH1")

    return new_bucket

def get_weather_data(BASE_URL, API_KEY):
    """ Creating the complete url from base url and API key, get the data from http end point in json format"""

    COMPLETE_URL = BASE_URL + API_KEY
    response_json = requests.get(url = COMPLETE_URL).json()
    return response_json

def clean_weather_data(weather_data):
    """ Cleanning the http json response"""

    response_dict = {}

    coordinate = {}
    coordinate["longitude"] = weather_data.get('coord').get('lon') if weather_data.get('coord') else None
    coordinate["latitude"] = weather_data.get('coord').get('lat') if weather_data.get('coord') else None
    response_dict["coordinate"] = coordinate

    response_dict["weather"] = {
        "id":weather_data.get('weather')[0].get('id') if weather_data.get('weather') else None,
        "main":weather_data.get('weather')[0].get('main') if weather_data.get('weather') else None,
        "description":weather_data.get('weather')[0].get('description') if weather_data.get('weather') else None
    }
    
    response_dict["base"] = weather_data.get("base")

    main={}
    main["temp"] = weather_data.get('main').get('temp') if weather_data.get('main') else None
    main["feels_like"] = weather_data.get('main').get('feels_like') if weather_data.get('main') else None
    main["pressure"] = weather_data.get('main').get('pressure') if weather_data.get('main') else None
    main["humidity"] = weather_data.get('main').get('humidity') if weather_data.get('main') else None
    main["temp_min"] = weather_data.get('main').get('temp_min') if weather_data.get('main') else None
    main["temp_max"] = weather_data.get('main').get('temp_max') if weather_data.get('main') else None
    main["sea_level"] = weather_data.get('main').get('sea_level') if weather_data.get('main') else None
    main["ground_level"] = weather_data.get('main').get('grnd_level') if weather_data.get('main') else None
    response_dict["main"] = main

    response_dict["visibility"] = weather_data.get('visibility')

    wind={}
    wind["speed"] = weather_data.get('wind').get('speed') if weather_data.get('wind') else None
    wind["degree"] = weather_data.get('wind').get('deg') if weather_data.get('wind') else None
    wind["gust"] = weather_data.get('wind').get('gust') if weather_data.get('wind') else None
    response_dict["wind"] = wind

    cloud={}
    cloud["all"] = weather_data.get('clouds').get('all') if weather_data.get('clouds') else None
    response_dict["clouds"] = cloud

    rain = {}
    rain["rain_1h"] = weather_data.get('rain').get('1h') if weather_data.get('rain') else None
    rain["rain_3h"] = weather_data.get('rain').get('3h') if weather_data.get('rain') else None
    response_dict["rain"] = rain

    snow={}
    snow["snow_1h"] = weather_data.get('snow').get('1h') if weather_data.get('snow') else None
    snow["snow_3h"] = weather_data.get('snow').get('3h') if weather_data.get('snow') else None

    response_dict["snow"]=snow

    response_dict["dt"] = current_date
    response_dict["current_time"] = current_time

    sys={}
    sys["country"] = weather_data.get('sys').get('country')  if weather_data.get('sys') else None
    sys["sunrise"] = weather_data.get('sys').get('sunrise')  if weather_data.get('sys') else None
    sys["sunset"] = weather_data.get('sys').get('sunset')  if weather_data.get('sys') else None
    response_dict["sys"] = sys

    response_dict["timezone"] = weather_data.get('timezone')
    response_dict["name"] = weather_data.get('name')
    
    return response_dict

def upload_file_to_gcs(new_bucket_name, source_file_name, destinatio_file_name):
    """ Upload the json file to GCS bucket """

    storage_client = storage.Client()
    terget_bucket = storage_client.bucket(new_bucket_name)
    blob = terget_bucket.blob(destinatio_file_name)
    blob.upload_from_filename(source_file_name)
    print(f"{source_file_name} has been uploaded to bucket {terget_bucket.name}")

def publish_massage(project_id, topic_id, message):
    """ Publish the log message into a GCP pubub topic """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    encoded_message = message.encode("utf-8")
    future = publisher.publish(topic_path, encoded_message)
    return future.result()

def main_pubsub(event, context):
    """ Main function triggered by pubsub mesage """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    if pubsub_message == "collect-weather-data":
        NEW_BUCKET_NAME = "openweather-hourly-weather-data-" + f"{PROJECT_ID}"

        if check_list_of_buckets(NEW_BUCKET_NAME) != True:
            new_bucket = create_new_bucket(NEW_BUCKET_NAME)
            print(f"new bucket created with name : {new_bucket.name}")

        API_KEY = get_api_key()

        try:
            hourly_weather_data_json = get_weather_data(BASE_URL, API_KEY)
            cleanned_weather_data = clean_weather_data(hourly_weather_data_json)

            temp_dir = '/tmp/'
            os.chdir(temp_dir)

            JSON_HOURLY_WEATHER_DATA = f'hourly-weather-data-json-{time_stamp}.json'
            
            with open(JSON_HOURLY_WEATHER_DATA, 'w') as outfile:
                json.dump(cleanned_weather_data, outfile)

            upload_file_to_gcs(NEW_BUCKET_NAME, JSON_HOURLY_WEATHER_DATA, JSON_HOURLY_WEATHER_DATA)
            os.remove(JSON_HOURLY_WEATHER_DATA)

            MESSAGE_DATA["file_name"] = JSON_HOURLY_WEATHER_DATA
            MESSAGE_DATA["gcs_uri"] = f'gs://{NEW_BUCKET_NAME}/{JSON_HOURLY_WEATHER_DATA}'
            MESSAGE_DATA["message_type"] = "success"
            MESSAGE_DATA["message"] = f'{JSON_HOURLY_WEATHER_DATA} successfully processed'
            
            SUCCESS_MESSAGE = json.dumps(MESSAGE_DATA)
            publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, SUCCESS_MESSAGE)

        except Exception as e:
            MESSAGE_DATA["file_name"] = JSON_HOURLY_WEATHER_DATA
            MESSAGE_DATA["gcs_uri"] = f'gs://{NEW_BUCKET_NAME}/{JSON_HOURLY_WEATHER_DATA}'
            MESSAGE_DATA["message_type"] = "error"
            MESSAGE_DATA['message'] = f"{e}"

            ERROR_MESSAGE = json.dumps(MESSAGE_DATA)
            publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, ERROR_MESSAGE)

    else:
        print("No bucket created")
