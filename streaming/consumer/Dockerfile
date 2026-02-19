FROM python:3.10-slim-bookworm

WORKDIR /app
COPY consumer.py .
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir kafka-python psycopg2-binary

CMD ["python", "consumer.py"]
