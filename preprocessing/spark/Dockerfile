FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Download NLTK data ────────────────────────────────────
RUN python -m nltk.downloader -d /usr/share/nltk_data \
    stopwords wordnet vader_lexicon punkt averaged_perceptron_tagger

# ── Pre-cache Hadoop-AWS dependency via Ivy ──────────────
# spark-submit --packages akan nge-download hadoop-aws + AWS SDK v2
# beserta transitive deps ke /root/.ivy2 agar runtime offline.
RUN echo 'import sys; sys.exit(0)' > /tmp/dummy.py && \
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.2 \
    --repositories https://repo1.maven.org/maven2 \
    --conf "spark.app.name=cache" /tmp/dummy.py 2>&1; \
    rm -f /tmp/dummy.py

COPY . .

RUN mkdir -p /app/jobs/models && \
    if [ ! -f /app/jobs/models/lid.176.ftz ]; then \
    wget -q https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz \
    -O /app/jobs/models/lid.176.ftz; \
    fi

RUN mkdir -p /app/logs
RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]