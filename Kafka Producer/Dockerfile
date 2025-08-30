# Dockerfile for Kafka Producer
FROM python:3.10-slim
WORKDIR /app
# Install dependencies first for better caching
COPY producer.py ./
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "producer.py"]
