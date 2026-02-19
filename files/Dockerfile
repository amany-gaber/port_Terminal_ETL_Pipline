FROM python:3.10-slim-bookworm

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy and install requirements first (better Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY etl_pipeline.py .

# Create data directory (will be mounted at runtime)
RUN mkdir -p /data

# Run the pipeline
CMD ["python", "etl_pipeline.py"]
