import json
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import numpy as np
from confluent_kafka import Consumer, Producer, KafkaException

from src.common.logger import get_logger

logger = get_logger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"

TRADE_TOPIC = "market.trades.raw"
FEATURE_TOPIC = "market.features.trade_activity"
REPLAY_TOPIC = "market.trades.replay"
DLQ_TOPIC = "market.errors.dlq"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "trade-feature-engine-group",
    "auto.offset.reset": "earliest",
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "trade-feature-engine-producer",
})

trades_by_symbol = defaultdict(deque)
prices_by_symbol = defaultdict(deque)
returns_by_symbol = defaultdict(deque)


def parse_time(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def now_utc():
    return datetime.now(timezone.utc)


def delivery_report(err, msg):
    if err is not None:
        print(f"Feature delivery failed: {err}")


def trim_deque_by_time(items, cutoff_time, timestamp_key="event_timestamp"):
    while items and parse_time(items[0][timestamp_key]) < cutoff_time:
        items.popleft()


def compute_vwap(trades):
    total_volume = sum(t["size"] for t in trades)

    if total_volume == 0:
        return None

    dollar_volume = sum(t["price"] * t["size"] for t in trades)
    return dollar_volume / total_volume


def compute_return_1m(symbol, current_price, current_time):
    prices = prices_by_symbol[symbol]

    if len(prices) < 2:
        return None

    target_time = current_time - timedelta(seconds=60)

    past_candidates = [
        item for item in prices
        if parse_time(item["event_timestamp"]) <= target_time
    ]

    if not past_candidates:
        return None

    past_price = past_candidates[-1]["price"]

    if past_price == 0:
        return None

    return (current_price - past_price) / past_price


def update_short_return(symbol, current_price, current_time):
    prices = prices_by_symbol[symbol]

    if len(prices) < 2:
        return

    previous_price = prices[-2]["price"]

    if previous_price == 0:
        return

    short_return = (current_price - previous_price) / previous_price

    returns_by_symbol[symbol].append({
        "event_timestamp": current_time.isoformat(),
        "return": short_return,
    })


def compute_realized_vol_5m(symbol, current_time):
    returns = returns_by_symbol[symbol]
    cutoff = current_time - timedelta(minutes=5)

    trim_deque_by_time(returns, cutoff)

    values = [r["return"] for r in returns]

    if len(values) < 2:
        return None

    return float(np.std(values, ddof=1))


def publish_feature(symbol, feature):
    producer.produce(
        FEATURE_TOPIC,
        key=symbol.encode("utf-8"),
        value=json.dumps(feature).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


def compute_and_publish_features(trade):
    symbol = trade["symbol"]
    current_price = trade["price"]
    current_time = now_utc()

    trades_by_symbol[symbol].append(trade)

    prices_by_symbol[symbol].append({
        "event_timestamp": current_time.isoformat(),
        "price": current_price,
    })

    trim_deque_by_time(
        trades_by_symbol[symbol],
        current_time - timedelta(seconds=60),
    )

    trim_deque_by_time(
        prices_by_symbol[symbol],
        current_time - timedelta(minutes=10),
    )

    update_short_return(symbol, current_price, current_time)

    trades_last_1m = trades_by_symbol[symbol]

    trade_count_1m = len(trades_last_1m)
    volume_1m = sum(t["size"] for t in trades_last_1m)
    dollar_volume_1m = sum(t["price"] * t["size"] for t in trades_last_1m)

    rolling_vwap_1m = compute_vwap(trades_last_1m)
    return_1m = compute_return_1m(symbol, current_price, current_time)
    realized_vol_5m = compute_realized_vol_5m(symbol, current_time)

    feature = {
        "symbol": symbol,
        "feature_timestamp": current_time.isoformat(),
        "last_trade_price": round(current_price, 4),
        "trade_count_1m": trade_count_1m,
        "volume_1m": volume_1m,
        "dollar_volume_1m": round(dollar_volume_1m, 2),
        "rolling_vwap_1m": round(rolling_vwap_1m, 4) if rolling_vwap_1m else None,
        "return_1m": round(return_1m, 6) if return_1m is not None else None,
        "realized_vol_5m": round(realized_vol_5m, 6) if realized_vol_5m is not None else None,
        "source": "trade_feature_engine",
    }

    publish_feature(symbol, feature)
    print(f"Published trade feature: {feature}")

def send_to_dlq(original_msg, error_message):
    bad_event = {
        "error": str(error_message),
        "raw_value": original_msg.value().decode("utf-8", errors="replace"),
        "topic": original_msg.topic(),
        "partition": original_msg.partition(),
        "offset": original_msg.offset(),
        "failed_at": now_utc().isoformat(),
    }

    producer.produce(
        DLQ_TOPIC,
        value=json.dumps(bad_event).encode("utf-8"),
    )
    producer.poll(0)


def handle_message(msg):
    trade = json.loads(msg.value().decode("utf-8"))

    if trade.get("event_type") != "trade":
        return

    if trade.get("symbol") is None:
        return

    if trade.get("price") is None or trade.get("size") is None:
        return

    compute_and_publish_features(trade)


def main():
    consumer.subscribe([TRADE_TOPIC, REPLAY_TOPIC])
    logger.info("Trade feature engine running.")
    

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        try:
            handle_message(msg)
        except Exception as e:
            logger.exception(f"Failed to process message: {e}")
            send_to_dlq(msg, e)

        finally:
            consumer.close()
            producer.flush()


if __name__ == "__main__":
    main()