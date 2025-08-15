from kafka import KafkaProducer

# Kafka broker address
BROKER = "localhost:9092"

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
    # Replace this with the message you want to send
    message_to_send = "Hello, Kafka!"
    print(f"Sending message: {message_to_send}")
    send_message(message_to_send)