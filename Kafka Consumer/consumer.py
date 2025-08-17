
import os
from kafka import KafkaConsumer

def consume_messages(topic, bootstrap_servers=None, group_id='test-consumer-group'):
	if bootstrap_servers is None:
		bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
	consumer = KafkaConsumer(
		topic,
		bootstrap_servers=bootstrap_servers,
		group_id=group_id,
		auto_offset_reset='earliest',
		enable_auto_commit=True
	)
	print(f"Consuming messages from topic: {topic}")
	try:
		for message in consumer:
			print(f"Received message: {message.value.decode('utf-8')}")
	except KeyboardInterrupt:
		print("Stopped consuming.")
	finally:
		consumer.close()

if __name__ == "__main__":
	topic = "test-topic"  # Change this to your topic name
	# Optionally set KAFKA_BOOTSTRAP_SERVERS env variable in Docker
	consume_messages(topic)