FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY yfinance_ws_producer.py .

CMD ["python", "yfinance_ws_producer.py"]
