from kafka import KafkaConsumer, KafkaProducer
import json
import csv
import time
import os
import argparse

if __name__ == '__main__':
    currentPath = os.path.dirname(os.path.realpath(__file__))+'/sample_citybikes.csv'
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help='Sample data file path',default=currentPath)
    parser.add_argument('-k', '--kafkaServers', help='Comma separated list of Kafka hosts',default='localhost:9092')
    parser.add_argument('-t', '--topic', help='Name of topic',default='citybikes')
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.kafkaServers)
    reader = csv.DictReader(open(args.file, 'r'))
    messageCount=0
    for line in reader:
        producer.send(args.topic, json.dumps(line).encode('utf-8'))
        messageCount=messageCount+1
        print ("Sent: "+str(messageCount))
        time.sleep(1)
