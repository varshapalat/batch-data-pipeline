from kafka import KafkaConsumer, KafkaProducer
import json
import csv
import time
import os
if __name__ == '__main__':
    currentPath = os.path.dirname(os.path.realpath(__file__))
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    reader = csv.DictReader(open(currentPath+'/sample_citybikes.csv', 'r'))
    messageCount=0
    for line in reader:
        producer.send('citybikes', json.dumps(line).encode('utf-8'))
        messageCount=messageCount+1
        print ("Sent: "+str(messageCount))
        time.sleep(1)
