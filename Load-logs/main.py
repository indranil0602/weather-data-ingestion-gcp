import base64
import os
import ast
import json
from datetime import datetime
from uuid import uuid4

from google.cloud import storage
from google.cloud import bigquery

CURRENT_DATE = datetime.now().strftime("%Y%m%d%H%M%S")
LOG_BUCKET = 'hourly-weather-data-load-logging'

tmp_dir = '/tmp/'
os.chdir(tmp_dir)

def upload_log_file_to_gcs(bucket_name, source_file_name, destination_file_name):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_file_name)
  blob.upload_from_filename(source_file_name)

def create_log_file_from_pubsub(pubsub_dict, current_date = CURRENT_DATE):
  pubsub_json = json.dumps(pubsub_dict)

  log_type = pubsub_dict.get('message_type')
  json_filename = f"hourly_weather_data_{log_type}_{current_date}_{uuid4()}.json"

  with open(json_filename, 'w') as logfile:
    logfile.write(pubsub_json)

  return json_filename

def main_logging(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_dict = ast.literal_eval(pubsub_message)

    log_file = create_log_file_from_pubsub(pubsub_dict)

    log_message_type = pubsub_dict.get('message_type')
    log_type = pubsub_dict.get('log_type')

    destination_log_file_name = log_type + "/" + log_message_type + "/" + log_file

    upload_log_file_to_gcs(LOG_BUCKET, log_file, destination_log_file_name)
    os.remove(log_file)
    
    print("log file uploaded")

