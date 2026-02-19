FROM python:3.10-slim-bookworm

WORKDIR /app
COPY producer.py .
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir kafka-python

CMD ["python", "producer.py"]
