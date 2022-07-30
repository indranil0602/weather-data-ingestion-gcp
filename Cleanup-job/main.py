import base64
import json
from datetime import datetime

# GCP imports
from google.cloud import bigquery
from google.cloud import pubsub_v1

PUBSUB_LOGGING_TOPIC = 'hourly-weather-data-load-logging-topic'
PUBSUB_LOGGING_SERVICE = 'Cloud Function'

f = open("config.json", "r")
data = json.load(f)

PROJECT_ID = data.get("project_id")
DATASET = data.get("dataset")
TABLE = data.get("table")

time_stamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")

# defining message data for the pubsub topic
MESSAGE_DATA = {
    "project":PROJECT_ID,
    "service":PUBSUB_LOGGING_SERVICE,
    "process":"Call Openweather Current Weather Data Api",
    "runtime":time_stamp,
    "log_type":"bq-cleanup-logging"
}

def publish_massage(project_id, topic_id, message):
    """ Publish the log message into a GCP pubub topic """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    encoded_message = message.encode("utf-8")
    future = publisher.publish(topic_path, encoded_message)
    return future.result()

def main_pubsub(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    if pubsub_message == 'cleanup-older-data':
        try:
            bq_client = bigquery.Client()

            qry = "CALL `data-ingestion-project-355102.raw_weather_data.sp_raw_cleanup_15days_older_data_query`();"
            job = bq_client.query(qry, location="ASIA-SOUTH1")

            MESSAGE_DATA["file_name"] = "file_name"
            MESSAGE_DATA["bq_uri"] = f'{PROJECT_ID}.{DATASET}.{TABLE}'
            MESSAGE_DATA["message_type"] = "success"
            MESSAGE_DATA["message"] = f'{PROJECT_ID}.{DATASET}.{TABLE} successfully cleanned'

            SUCCESS_MESSAGE = json.dumps(MESSAGE_DATA)
            publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, SUCCESS_MESSAGE)

        except Exception as e:
            MESSAGE_DATA["file_name"] = "file_name"
            MESSAGE_DATA["bq_uri"] = f'{PROJECT_ID}.{DATASET}.{TABLE}'
            MESSAGE_DATA["message_type"] = "Error"
            MESSAGE_DATA["message"] = f'{e}'

            ERROR_MESSAGE = json.dumps(MESSAGE_DATA)
            publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, ERROR_MESSAGE)
    else:
        print(pubsub_message)
