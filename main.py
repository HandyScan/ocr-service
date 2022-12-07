from PIL import Image
from pytesseract import pytesseract
import os
import io
from wand.image import Image as wi
from flask import Flask, request, Response, send_file
import redis
import jsonpickle
import hashlib
from minio import Minio

# intialize the application
app = Flask(__name__)

def pdfToText(file_name):
    pdfFile = wi(filename = file_name, resolution = 300)
    # pdfFile = wi(filename = 'images/Passport-Puneeth-merged.pdf', resolution = 300)
    image = pdfFile.convert('jpeg')

    imageBlobs = []

    for img in image.sequence:
        imgPage = wi(image = img)
        imageBlobs.append(imgPage.make_blob('jpeg'))

    extract = ''

    for imgBlob in imageBlobs:
        image = Image.open(io.BytesIO(imgBlob))
        text = pytesseract.image_to_string(image, lang = 'eng')
        # re.sub('[^a-zA-Z0-9 \n\.]', '', text)
        # extract.append(text)
        extract = extract + text

    print(extract)

#Define path to tessaract.exe
# path_to_tesseract = r'/usr/local/Cellar/tesseract/5.2.0/bin/tesseract'

#Define path to image
path_to_images = r'images/'

#Point tessaract_cmd to tessaract.exe
# pytesseract.tesseract_cmd = path_to_tesseract

for root, dirs, file_names in os.walk(path_to_images):
    #Iterate over each file_name in the folder
    for file_name in file_names:
        if file_name.endswith(".pdf"):
            pdfToText(path_to_images + file_name)
        else:
            #Open image with PIL
            img = Image.open(path_to_images + file_name)

            #Extract text from image
            text = pytesseract.image_to_string(img)

            print(text)

if __name__ == '__main__':
  app.run(host="0.0.0.0", port=5000)