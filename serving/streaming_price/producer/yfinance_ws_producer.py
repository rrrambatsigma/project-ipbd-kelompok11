import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from loguru import logger

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market.eurusd.raw")
SYMBOL = os.getenv("YFINANCE_SYMBOL", "EURUSD=X")
INTERVAL = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "5"))

# Keep demo alive if yfinance stream fails.
ENABLE_DUMMY_FALLBACK = os.getenv("ENABLE_DUMMY_FALLBACK", "true").lower() == "true"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def epoch_ms_to_iso(value) -> str:
    try:
        # yfinance websocket often sends time in milliseconds
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return utc_now()


def make_kafka_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                retries=5,
            )
            logger.success(f"Connected to Kafka: {BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka not ready: {e}. Retrying in 5s...")
            time.sleep(5)


def send_event(producer: KafkaProducer, event: dict) -> None:
    producer.send(TOPIC, key="EUR/USD", value=event)
    producer.flush()
    logger.info(f"sent {event}")


def normalize_yfinance_message(message: dict) -> dict | None:
    """
    Expected yfinance message is a dict containing at least symbol/id and price.
    The exact payload can vary, so this function is defensive.
    """
    symbol = message.get("id") or message.get("symbol") or SYMBOL
    price = message.get("price") or message.get("regularMarketPrice")

    if price is None:
        logger.warning(f"yfinance message without price, skipped: {message}")
        return None

    try:
        price = float(price)
    except Exception:
        logger.warning(f"invalid price, skipped: {message}")
        return None

    event_time = epoch_ms_to_iso(message.get("time")) if message.get("time") else utc_now()

    return {
        "symbol": symbol,
        "canonical_symbol": "EUR/USD",
        "instrument_type": "spot",
        "source": "yfinance_websocket",
        "price": round(price, 6),
        "bid": None,
        "ask": None,
        "volume": float(message["dayVolume"]) if message.get("dayVolume") is not None else None,
        "event_time": event_time,
        "ingestion_time": utc_now(),
        "quality_status": "ok",
    }


def run_yfinance_ws(producer: KafkaProducer) -> None:
    import yfinance as yf

    logger.info(f"Starting yfinance WebSocket producer for {SYMBOL} → topic={TOPIC}")

    def message_handler(message):
        try:
            event = normalize_yfinance_message(message)
            if event:
                send_event(producer, event)
        except Exception as e:
            logger.exception(f"Failed handling yfinance message: {e}")

    while True:
        try:
            with yf.WebSocket(verbose=False) as ws:
                ws.subscribe([SYMBOL])
                logger.success(f"Subscribed to yfinance symbol: {SYMBOL}")
                ws.listen(message_handler)
        except Exception as e:
            logger.exception(f"yfinance WebSocket failed: {e}")
            if not ENABLE_DUMMY_FALLBACK:
                time.sleep(10)
                continue

            logger.warning("Switching to dummy fallback for demo continuity.")
            run_dummy_fallback(producer)


def run_dummy_fallback(producer: KafkaProducer) -> None:
    price = 1.1700
    logger.info(f"Starting dummy EUR/USD fallback → topic={TOPIC}")

    while True:
        price += random.uniform(-0.0008, 0.0008)
        price = max(price, 0.8)

        event = {
            "symbol": SYMBOL,
            "canonical_symbol": "EUR/USD",
            "instrument_type": "spot_demo",
            "source": "dummy_fallback",
            "price": round(price, 6),
            "bid": None,
            "ask": None,
            "volume": None,
            "event_time": utc_now(),
            "ingestion_time": utc_now(),
            "quality_status": "fallback",
        }

        send_event(producer, event)
        time.sleep(INTERVAL)


def main():
    producer = make_kafka_producer()

    try:
        run_yfinance_ws(producer)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")


if __name__ == "__main__":
    main()
