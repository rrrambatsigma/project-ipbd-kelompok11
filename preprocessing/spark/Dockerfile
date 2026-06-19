FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Pre-download jar Hadoop-AWS & AWS SDK saat BUILD ───────
# Supaya Spark gak perlu akses internet lagi tiap kali container jalan
RUN mkdir -p /opt/spark-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O /opt/spark-jars/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -O /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar

COPY . .

RUN mkdir -p /app/jobs/models && \
    if [ ! -f /app/jobs/models/lid.176.ftz ]; then \
    wget -q https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz \
    -O /app/jobs/models/lid.176.ftz; \
    fi

RUN mkdir -p /app/logs

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["tail", "-f", "/dev/null"]