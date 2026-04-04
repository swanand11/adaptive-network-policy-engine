FROM python:3.11-slim

WORKDIR /app

COPY . /app

ENV PYTHONPATH=/app

RUN pip install -r requirements.txt

CMD ["python", "-m", "kafka_core.prometheus_kafka_adaptar"]