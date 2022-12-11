FROM python:3.10-slim

WORKDIR /app

COPY service_worker.py /app
COPY requirements.txt  /app

RUN pip3 install -r requirements.txt
RUN apt-get update \
  && apt-get -y install tesseract-ocr
RUN ls -altr  /usr/bin/tesseract

ENV PYTHONUNBUFFERED=1
# CMD [ "python3", "worker.py"]
ENTRYPOINT ["python3", "service_worker.py"]
