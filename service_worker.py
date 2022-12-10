from PIL import Image
from pytesseract import pytesseract
import os
import sys
import io
# from wand.image import Image as wi
from flask import Flask, request, Response, send_file
import logging
import redis
import jsonpickle
import hashlib
from minio import Minio
from confluent_kafka import Producer, Consumer

# intialize the application
app = Flask(__name__)

# GLOBAL VARS
path_to_tesseract = os.environ.get('TESSERACT_PATH', r'E:\tesseract\tesseract.exe')

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# tesseract
pytesseract.tesseract_cmd = path_to_tesseract


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

# Minio Config
minioHost = os.getenv("MINIO_HOST") or "127.0.0.1:9000"
minioUser = os.getenv("MINIO_USER") or "minioadmin"
minioPasswd = os.getenv("MINIO_PASSWD") or "minioadmin"
print(f"Getting minio connection now for host {minioHost}!")

minio_client = None
try:
    minio_client = Minio(minioHost, access_key=minioUser, secret_key=minioPasswd, secure=False)
    print("Got minio connection",minio_client )
except Exception as exp:
    print(f"Exception raised in worker loop: {str(exp)}")

def get_details_from_kafka():
    consumer.subscribe(['ocr_local_topic'])
    file_detail_msg = consumer.poll(1.0)
    if file_detail_msg is None:
        return
    if file_detail_msg.error():
        logger.error('ERROR: Failed to get file details from OCR_TOPIC', str(file_detail_msg.error()))
        return
    file_detail = file_detail_msg.value().decode('utf-8')
    logger.info("Fetched File to process: " + file_detail)

    return jsonpickle.loads(file_detail)


def get_file(file_detail):
    file_path = os.path.join("tmp\input", file_detail['file_name'])
    logger.info(file_path)
    minio_client.fget_object(file_detail['bucket'], file_detail['file_name'], file_path)
    return file_path


def get_text_from_tesseract(img_file_path):
    if img_file_path:
        img = Image.open(img_file_path)
        logger.info("Sending Image to tesseract")
        text = pytesseract.image_to_string(img)
        logger.info(text)
        return text

def write_text_file(text, file_name):
    if len(text) != 0:
        content = io.BytesIO(bytes(text, 'utf-8'))
        response = minio_client.put_object('tts-bucket', file_name, content, content.getbuffer().nbytes)
        logger.info(response)
        return response

def kafka_produce_receipt(err,msg):
    if err is not None:
        logger.error('Error: {}'.format(err))
    else:
        message = 'SUCCESS: Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)

def send_kafka_tts_message(tts_file_detail):
    producer.poll(1.0)
    producer.produce('tts_topic', value=jsonpickle.dumps(tts_file_detail), callback=kafka_produce_receipt)
    return

while True:
    file_detail = get_details_from_kafka()
    file_path = None
    text = ''
    if file_detail:
        file_path = get_file(file_detail)
        if file_path.endswith('pdf'):
        # img_file = pdf_to_image(file_path)
        # get_text_from_tesseract(img_file)
            continue
        else:
            text = get_text_from_tesseract(file_path)
        if len(text) != 0:
            tts_file_name = file_detail['file_name'].split(".")[0]+".txt"
            write_text_file(text, tts_file_name)
            tts_file_detail = file_detail
            tts_file_detail['file_name'] = tts_file_name
            tts_file_detail['bucket'] = 'tts-bucket'
            logger.info("TTS Kafka Message: ", tts_file_detail)
            send_kafka_tts_message(tts_file_detail)
