import json
import os
import logging
from os import getenv

import pendulum

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


"""{topic_name: {num_partitions: int (default -1),
              replication_factor: int (default -1),
              assignments: {partition_id: [broker_ids]}"""
default_topic_config = {
    "num_partitions": 1,
    "replication_factor": 1
}

def create_topic(topic_name: str = None) -> NewTopic:
    if topic_name is None:
        raise ValueError("Topic name does not empty!")
    kafka_admin = AdminClient(conf={
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    })
    topic = NewTopic(topic=topic_name,
                     num_partitions=5,
                     replication_factor=2,
                     config={"min.insync.replicas": "2"}
                     )
    kafka_admin.create_topics([topic])
    l = kafka_admin.list_topics()
    logger.info(l.topics)
    return topic


def produce(topic: NewTopic, n: int = 1):
    producer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "acks": "all",  # Для синхронной репликации
        "retries": 2,   # Количество попыток при сбоях
    }
    producer = Producer(**producer_config)

    for _ in range(n):

        key = "key".encode("utf-8")
        value = json.dumps({
            "event_timestamp": pendulum.now().timestamp(),
            "event_datetime": pendulum.now(tz="local").to_iso8601_string()
        }).encode("utf-8")

        producer.produce(topic=topic.topic,
                      key=key,
                      value=value)
    producer.flush()


if __name__ == "__main__":
    topic = create_topic(topic_name="t2.l6.topic.part.5")
    logger.info(topic)
    produce(topic=topic)