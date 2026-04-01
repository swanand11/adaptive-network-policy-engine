FROM python:3.11-slim

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8001 8002 8003 9090

CMD ["python", "-m", "mocks.aws_simulator"]