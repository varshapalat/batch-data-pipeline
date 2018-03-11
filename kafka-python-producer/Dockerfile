FROM python:3.6

WORKDIR /usr/src/app

COPY . .

RUN pip3 install -r requirements.txt

CMD python kafka_producer.py -k docker.for.mac.host.internal:9092
