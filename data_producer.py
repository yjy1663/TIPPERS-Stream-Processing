#read testing json rawdata
#simulate rawdata as realtime data stream 
#connecting broker and send data

#docker command to run zookeeper container 
#docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper

# docker command to run kafka broker container
# docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME = localhost -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka

#command to run producer to send data to localhost kafka broker topic WifiAP
#python WIFI_sensordata_producer.py WiFiAP.json WifiAP localhost:9092

#kafka-topics.sh --zookeeper localhost:2181 --list

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import json
import time
import logging
import atexit

#logging setup
logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

rawdata = 'WiFiAP.json'
topic_name = 'WifiAP'
kafka_broker = 'localhost:9092'

#reading data from json file later will replace by real stream
def fetch_data(producer, rawdata):
	with open(rawdata) as data_file:
		data = json.load(data_file)

	for data_object in data["rows"]:
		data_string = b'%s, %s, %s, %s, %s' %(data_object["timeStamp"], 
																				data_object["sensor_id"], 
																				data_object["observation_type_id"], 
																				data_object["id"], 
																				data_object["payload"]["client_id"])
		try:
			logger.debug('received Wifi observation data from %s' % data_object["payload"]["client_id"])
			data_string = json.dumps(data_string)
			producer.send(topic_name, data_string)
		except KafkaTimeoutError as timeout_error:
			logger.warn('time out error failed to send Wifi observation data to kafka')
		except Exception:
			logger.warn('failed to send Wifi observation data')
		time.sleep(1)

#Release resource
def shutdown_hook():
	try:
		producer.flush(10)
		logger.info('shutdown resources')
	except KafkaError as ke:
		logger.warn('failed to flush kafka')
	finally:
		producer.close(10)

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('rawdata', help='input rawdata file')
	parser.add_argument('topic_name', help='kafka topic')
	parser.add_argument('kafka_broker', help='location of broker')

	args = parser.parse_args()
	rawdata = args.rawdata
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	producer = KafkaProducer(bootstrap_servers=kafka_broker)
	fetch_data(producer, rawdata)

	atexit.register(shutdown_hook)
