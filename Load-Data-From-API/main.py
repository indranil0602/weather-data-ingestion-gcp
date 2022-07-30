import os
import json
import base64
import requests
from datetime import datetime
from pytz import timezone 
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import pubsub_v1

time_stamp = datetime.now(timezone("Asia/Kolkata")).strftime("%Y%m%d-%H:%M:%S")
current_date = datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
current_time = datetime.now(timezone("Asia/Kolkata")).strftime("%H:%M:%S")
current_year = datetime.now(timezone("Asia/Kolkata")).strftime("%Y")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather?q=Bankura,WB,IN&appid="

f = open("config.json", "r")
data = json.load(f)
PROJECT_ID = data.get("project_id")

PUBSUB_LOGGING_TOPIC = 'hourly-weather-data-load-logging-topic'
PUBSUB_LOGGING_SERVICE = 'Cloud Function'

MESSAGE_DATA = {
    "project":PROJECT_ID,
    "service":PUBSUB_LOGGING_SERVICE,
    "process":"Call Openweather Current Weather Data Api",
    "runtime":time_stamp,
    "log_type":"api-logging"
}

def get_api_key():
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = data.get("secret_name")
    request = {"name" : f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"}
    response = secret_client.access_secret_version(request)
    API_KEY = response.payload.data.decode("UTF-8") 
    return API_KEY  

def check_list_of_buckets(bucket_name):
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        if bucket_name == bucket.name:
            return True
    return False 

def create_new_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="ASIA-SOUTH1")
    return bucket

def get_weather_data(BASE_URL, API_KEY):
    COMPLETE_URL = BASE_URL + API_KEY
    response_json = requests.get(url = COMPLETE_URL).json()
    return response_json

    
def clean_weather_data(data):
    response_dict = {}

    coordinate = {}
    coordinate["longitude"] = data.get('coord').get('lon') if data.get('coord') else None
    coordinate["latitude"] = data.get('coord').get('lat') if data.get('coord') else None
    response_dict["coordinate"] = coordinate

    response_dict["weather"] = {
        "id":data.get('weather')[0].get('id') if data.get('weather') else None,
        "main":data.get('weather')[0].get('main') if data.get('weather') else None,
        "description":data.get('weather')[0].get('description') if data.get('weather') else None
    }

    response_dict["base"] = data.get("base")

    main={}
    main["temp"] = data.get('main').get('temp') if data.get('main') else None
    main["feels_like"] = data.get('main').get('feels_like') if data.get('main') else None
    main["pressure"] = data.get('main').get('pressure') if data.get('main') else None
    main["humidity"] = data.get('main').get('humidity') if data.get('main') else None
    main["temp_min"] = data.get('main').get('temp_min') if data.get('main') else None
    main["temp_max"] = data.get('main').get('temp_max') if data.get('main') else None
    main["sea_level"] = data.get('main').get('sea_level') if data.get('main') else None
    main["ground_level"] = data.get('main').get('grnd_level') if data.get('main') else None
    response_dict["main"] = main

    response_dict["visibility"] = data.get('visibility')

    wind={}
    wind["speed"] = data.get('wind').get('speed') if data.get('wind') else None
    wind["degree"] = data.get('wind').get('deg') if data.get('wind') else None
    wind["gust"] = data.get('wind').get('gust') if data.get('wind') else None
    response_dict["wind"] = wind

    cloud={}
    cloud["all"] = data.get('clouds').get('all') if data.get('clouds') else None
    response_dict["clouds"] = cloud

    rain = {}
    rain["rain_1h"] = data.get('rain').get('1h') if data.get('rain') else None
    rain["rain_3h"] = data.get('rain').get('3h') if data.get('rain') else None
    response_dict["rain"] = rain

    snow={}
    snow["snow_1h"] = data.get('snow').get('1h') if data.get('snow') else None
    snow["snow_3h"] = data.get('snow').get('3h') if data.get('snow') else None
    response_dict["snow"]=snow

    response_dict["dt"] = current_date
    response_dict["current_time"] = current_time

    sys={}
    sys["country"] = data.get('sys').get('country')  if data.get('sys') else None
    sys["sunrise"] = data.get('sys').get('sunrise')  if data.get('sys') else None
    sys["sunset"] = data.get('sys').get('sunset')  if data.get('sys') else None
    response_dict["sys"] = sys

    response_dict["timezone"] = data.get('timezone')
    response_dict["id"] = data.get('id')
    response_dict["name"] = data.get('name')
    
    return response_dict

def upload_file_to_gcs(new_bucket_name, source_file_name, destinatio_file_name):
    storage_client = storage.Client()
    terget_bucket = storage_client.bucket(new_bucket_name)
    blob = terget_bucket.blob(destinatio_file_name)
    blob.upload_from_filename(source_file_name)
    print(f"{source_file_name} has been uploaded to bucket {terget_bucket.name}")

def publish_massage(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    encoded_message = message.encode("utf-8")
    future = publisher.publish(topic_path, encoded_message)
    return future.result()

def main_pubsub(event, context):
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

