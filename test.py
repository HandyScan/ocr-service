from confluent_kafka import Producer, Consumer
from pymongo import MongoClient
from pytesseract import pytesseract
import os
import sys
import io
import logging
import jsonpickle
from minio import Minio
import unittest
from unittest.mock import MagicMock
from service_worker import *

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# tesseract
path_to_tesseract = os.environ.get('TESSERACT_PATH', r'E:\tesseract\tesseract.exe')
pytesseract.tesseract_cmd = path_to_tesseract

# Mongo DB
monogo_user = os.environ.get("MONGO_USERNAME", "handyscan_admin")
mongo_pwd = os.environ.get("MONGO_PASSWORD", "")
mongo_connection_url = "mongodb+srv://" + monogo_user + ":" + mongo_pwd + "@glasscode.8iobb.mongodb.net/handyscan?retryWrites=true&w=majority"
client = MongoClient(mongo_connection_url)
db = client.handyscan
print("Connected to Mongo")


# kafka Config
kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP', 'pkc-4r087.us-west2.gcp.confluent.cloud:9092')
kafka_api_key = os.environ.get('KAFKA_API_KEY', '4BRDEW6OW6Z35KZK')
kafka_secret_key = os.environ.get('KAFKA_SECRET_KEY', '')
kafka_config = {
'bootstrap.servers':'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
'security.protocol':'SASL_SSL',
'sasl.mechanisms':'PLAIN',
'sasl.username': kafka_api_key,
'sasl.password': kafka_secret_key,
'group.id':'ocr_consumer',
'auto.offset.reset':'earliest'
}

producer = Producer(kafka_config)
consumer =  Consumer(kafka_config)
print("Connected to kafka")


class TestOCR(unittest.TestCase):
    def test_file_details_kafka():
        consumer.poll = MagicMock(return_value='''{
            "user_name": "test",
            "bucket": "ocr-bucket",
            "file_name": "test.JPEG",
            "collection": "algo"
        }''')
        file_detail_msg = get_details_from_kafka()
        assert file_detail_msg['file_name'] == "test"
        assert file_detail_msg['bucket'] == "ocr-bucket"

    def test_update_metadata():
        file_details = {
            "user_name": "test",
            "bucket": "ocr-bucket",
            "file_name": "test.JPEG",
            "collection": "algo"
        }
        updated = update_metadata(file_details)
        assert updated[file_details['textFile']] == "test.txt"
        assert updated[file_details['status']] == "TTS"


