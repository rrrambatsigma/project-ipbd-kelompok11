FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schema.py .
COPY eurusd_stream_job.py .

CMD ["python", "eurusd_stream_job.py"]
