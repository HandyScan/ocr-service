from PIL import Image
from pytesseract import pytesseract
import os
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

# logging for kafka
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename='producer.log', filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP', 'pkc-4r087.us-west2.gcp.confluent.cloud:9092')
kafka_api_key = os.environ.get('KAFKA_API_KEY', '4BRDEW6OW6Z35KZK')
kafka_secret_key = os.environ.get('KAFKA_SECRET_KEY', '')
kafka_config = {
    'bootstrap.servers':'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username': kafka_api_key,
    'sasl.password': kafka_secret_key,
    'group.id':'ocr-consumer',
    'auto.offset.reset':'earliest'
}

producer = Producer(kafka_config)
consumer =  Consumer(kafka_config)

print("Connected to kafka")

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
    consumer.subscribe(['ocr_topic'])
    file_detail_msg = consumer.poll(1.0)
    if file_detail_msg is None:
        return
    if file_detail_msg.error():
        logger.error('ERROR: Failed to get file details from OCR_TOPIC', str(file_detail_msg.error()))
        return
    file_detail = file_detail_msg.value().decode('utf-8')
    logger.info("Fetched File to process: {}", file_detail)
    return file_detail

while True:
    get_details_from_kafka()
# def get_file_from_minio(file_details):
#     file_details

# def pdfToText(file_name):
#     pdfFile = wi(filename = file_name, resolution = 300)
#     # pdfFile = wi(filename = 'images/Passport-Puneeth-merged.pdf', resolution = 300)
#     image = pdfFile.convert('jpeg')

#     imageBlobs = []

#     for img in image.sequence:
#         imgPage = wi(image = img)
#         imageBlobs.append(imgPage.make_blob('jpeg'))

#     extract = ''

#     for imgBlob in imageBlobs:
#         image = Image.open(io.BytesIO(imgBlob))
#         text = pytesseract.image_to_string(image, lang = 'eng')
#         # re.sub('[^a-zA-Z0-9 \n\.]', '', text)
#         # extract.append(text)
#         extract = extract + text

#     print(extract)

# #Define path to tessaract.exe
# # path_to_tesseract = r'/usr/local/Cellar/tesseract/5.2.0/bin/tesseract'

# #Define path to image
# path_to_images = r'images/'

# #Point tessaract_cmd to tessaract.exe
# # pytesseract.tesseract_cmd = path_to_tesseract

# for root, dirs, file_names in os.walk(path_to_images):
#     #Iterate over each file_name in the folder
#     for file_name in file_names:
#         if file_name.endswith(".pdf"):
#             pdfToText(path_to_images + file_name)
#         else:
#             #Open image with PIL
#             img = Image.open(path_to_images + file_name)

#             #Extract text from image
#             text = pytesseract.image_to_string(img)

#             print(text)

# if __name__ == '__main__':
#   app.run(host="0.0.0.0", port=5000)