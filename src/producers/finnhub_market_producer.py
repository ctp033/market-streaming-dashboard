import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv
from websocket import WebSocketApp

from src.common.logger import get_logger

logger = get_logger(__name__)

load_dotenv()

BOOTSTRAP_SERVERS = "localhost:9092"
TRADE_TOPIC = "market.trades.raw"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

SYMBOLS = ["AAPL", "MSFT", "NVDA", "SPY"]

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "finnhub-trade-producer",
})


def unix_ms_to_iso(ts_ms):
    if ts_ms is None:
        return datetime.now(timezone.utc).isoformat()

    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Sent to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def normalize_trade(trade):
    """
    Finnhub trade fields:
    s = symbol
    p = price
    v = volume/size
    t = Unix timestamp in milliseconds
    c = trade conditions
    """
    return {
        "symbol": trade.get("s"),
        "event_type": "trade",
        "price": trade.get("p"),
        "size": trade.get("v"),
        "conditions": trade.get("c", []),
        "event_timestamp": unix_ms_to_iso(trade.get("t")),
        "finnhub_timestamp_ms": trade.get("t"),
        "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "finnhub",
    }


def send_trade_to_kafka(event):
    symbol = event["symbol"]

    producer.produce(
        TRADE_TOPIC,
        key=symbol.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


def on_open(ws):
    print("Connected to Finnhub WebSocket.")

    for symbol in SYMBOLS:
        subscribe_message = {
            "type": "subscribe",
            "symbol": symbol,
        }

        ws.send(json.dumps(subscribe_message))
        print(f"Subscribed to {symbol}")


def on_message(ws, message):
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        print(f"Could not decode message: {message}")
        return

    msg_type = payload.get("type")

    if msg_type == "ping":
        return

    if msg_type != "trade":
        print(f"Non-trade message: {payload}")
        return

    trades = payload.get("data", [])

    for trade in trades:
        event = normalize_trade(trade)

        if event["symbol"] is None:
            continue

        send_trade_to_kafka(event)
        print(f"Trade -> Kafka: {event}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")
    producer.flush()


def main():
    if not FINNHUB_API_KEY:
        raise RuntimeError("Missing FINNHUB_API_KEY. Add it to your .env file.")

    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    ws = WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    logger.info("Starting Finnhub trade producer. Press Ctrl+C to stop.")


    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping Finnhub producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()