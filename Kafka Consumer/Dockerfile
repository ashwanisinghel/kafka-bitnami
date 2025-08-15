# Dockerfile for Kafka Consumer
FROM python:3.10-slim
WORKDIR /app
# Install dependencies first for better caching
COPY consumer.py ./
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "consumer.py"]
