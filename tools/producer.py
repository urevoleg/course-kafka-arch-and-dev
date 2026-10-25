"""Kafka producer for random user events.

Run from the repository root:
    .venv/bin/python tools/producer.py <topic_name>

Example:
    .venv/bin/python tools/producer.py dev.topic.events.v1

Optional environment variables:
    KAFKA_BOOTSTRAP_SERVERS  Kafka brokers (default: 127.0.0.1:9092,127.0.0.1:9093)
    KAFKA_TOPIC              Topic if no positional argument is passed
    PRODUCER_INTERVAL_SECONDS Delay between messages (default: 1)
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

USERS = [f"user{number:03d}" for number in range(1, 101)]
EVENT_TYPES = ("purchase", "view", "cart_add")


def generate_event() -> dict[str, str | float]:
    """Create an event with one of 100 fixed user identifiers."""
    return {
        "user_id": random.choice(USERS),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now().isoformat(timespec="microseconds"),
        "value": round(random.uniform(1.0, 1_000.0), 2),
    }


def delivery_report(error, message) -> None:
    if error is not None:
        logger.error("Message delivery failed: %s", error)
    else:
        logger.info("Message delivered to %s [%s]", message.topic(), message.partition())


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate random user events for Kafka.")
    parser.add_argument(
        "topic",
        nargs="?",
        default=os.getenv("KAFKA_TOPIC", "dev.topic.events.v1"),
        help="Kafka topic to write to",
    )
    args = parser.parse_args()

    topic = args.topic
    interval_seconds = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
    producer = Producer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093"
            ),
            "acks": 1,
            "retries": 3,
        }
    )

    try:
        while True:
            event = generate_event()
            producer.produce(
                topic=topic,
                key=event["user_id"],
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            logger.info("Sent: %s", event)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
