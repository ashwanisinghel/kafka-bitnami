
import os
import json
from kafka import KafkaProducer


# Kafka broker address (can be set via environment variable)
# BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BROKER = 'localhost:9092'
print(f"Using Kafka broker: {BROKER}")

# Kafka topic to send messages to
TOPIC = "test-topic"  # Replace with your topic name

def send_message(message):
    # Initialize the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda x: x.encode('utf-8')  # Serialize string messages to bytes
    )
    
    try:
        # Send the message to the Kafka topic
        future = producer.send(TOPIC, value=message)
        # Block until the message is sent (or an error occurs)
        metadata = future.get(timeout=10)
        print(f"Message sent to topic '{metadata.topic}' on partition {metadata.partition}")
    except Exception as e:
        print(f"Failed to send message: {e}")
    finally:
        # Close the producer
        producer.close()

if __name__ == "__main__":
    # Send 100 sample JSON messages
    for i in range(1, 101):
        sample_data = {
            "id": i,
            "name": f"SampleName{i}",
            "value": i * 10
        }
        message_json = json.dumps(sample_data)
        print(f"Sending message: {message_json}")
        send_message(message_json)
    print("Finished sending 100 sample JSON messages.")