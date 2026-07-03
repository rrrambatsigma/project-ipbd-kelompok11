FROM python:3.11-slim

# Install curl untuk healthcheck dan dependency build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# PYTHONPATH agar flows/ bisa import scrapers/, storage/, utils/
ENV PYTHONPATH=/app/flows

CMD ["python", "-c", "print('Collector app is ready')"]
