import re
import os
import json
import base64
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud.exceptions import NotFound

f = open("config.json", "r")
data = json.load(f)

FILE_TABLE_MAP = {
    'hourly-weather-data' : 'weather-data-hourly'
}

PROJECT_ID = data.get("project_id")
DATASET = data.get("dataset")

PUBSUB_LOGGING_TOPIC = 'hourly-weather-data-load-logging-topic'
PUBSUB_LOGGING_SERVICE = 'Cloud Function'

time_stamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")

MESSAGE_DATA = {
    "project":PROJECT_ID,
    "service":PUBSUB_LOGGING_SERVICE,
    "process":"Call Openweather Current Weather Data Api",
    "runtime":time_stamp,
    "log_type":"bq-load-logging"
}

def build_gcs_uri(bucket, file_path):
     return f"gs://{bucket}/{file_path}"

def cleanup_file_name(file_name):
    datetime_pattern = '-json-\d{8}-\d{2}:\d{2}:\d{2}'

    match = re.findall(datetime_pattern, file_name)

    for matched_str in match:
        if len(matched_str) > 0:
            file_name = file_name.replace(matched_str, "").strip()

    return file_name

def publish_massage(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    encoded_message = message.encode("utf-8")
    future = publisher.publish(topic_path, encoded_message)
    return future.result()

def main_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
    event (dict): Event payload.
    context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket = event['bucket']
    gcs_uri = build_gcs_uri(bucket, event['name'])
    file_name = event['name']
    cleanned_filename = cleanup_file_name(file_name)
    file_stem = Path(cleanned_filename).stem
    load_table = FILE_TABLE_MAP.get(file_stem)

    try:
        if load_table:
            bq_client = bigquery.Client()
            table_ref = bq_client.dataset(DATASET).table(load_table)

            job_config = bigquery.LoadJobConfig(
                source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
            job_config.write_disposition = "WRITE_APPEND"

            load_job = bq_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                location="ASIA-SOUTH1",
                job_config=job_config
            )

            qry = ""
            try:
                bq_client.get_table(f'{PROJECT_ID}.transformed_weather_data.weather-data-daily')
                qry = "CALL `data-ingestion-project-355102.transformed_weather_data.sp_raw_to_transformed_update_weather_data_query`();"
            except NotFound:
                qry = "CALL `data-ingestion-project-355102.transformed_weather_data.sp_raw_to_transformed_create_weather_data_query`();"
            job = bq_client.query(qry, location="ASIA-SOUTH1")

            print(file_stem, file_name, gcs_uri)

            MESSAGE_DATA["file_name"] = file_name
            MESSAGE_DATA["bq_uri"] = f'{PROJECT_ID}.{DATASET}.{load_table}'
            MESSAGE_DATA["message_type"] = "success"
            MESSAGE_DATA["message"] = f'{file_name} successfully processed'

            SUCCESS_MESSAGE = json.dumps(MESSAGE_DATA)
            publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, SUCCESS_MESSAGE)
    except Exception as e:
        MESSAGE_DATA["file_name"] = file_name
        MESSAGE_DATA["bq_uri"] = f'{PROJECT_ID}.{DATASET}.{load_table}'
        MESSAGE_DATA["message_type"] = "error"
        MESSAGE_DATA["message"] = f'{e}'

        ERROR_MESSAGE = json.dumps(MESSAGE_DATA)
        publish_massage(PROJECT_ID, PUBSUB_LOGGING_TOPIC, ERROR_MESSAGE)

