from datetime import datetime
from enum import Enum
import os
import logging
import json

from pydantic import BaseModel, Field
import typing as t

from confluent_kafka import DeserializingConsumer, Consumer, KafkaError, Message

from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


consumer_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093"),
    'group.id': 'consumer_batch',
    'enable.auto.commit': 'false',
    'enable.auto.offset.store': 'true',
    'auto.offset.reset': 'latest',
    'fetch.min.bytes': 1048576, # 1MB
    'fetch.wait.max.ms': 30000
}


schema_registry_config = {
   'url': os.getenv("SCHEMA_REGISTRY_URL", 'http://localhost:8081')
}

# ---- Немного дублирования кода -----

class FreshnessStatus(str, Enum):
    """Статус проверки свежести данных"""
    PASS = "pass"
    WARN = "warn"
    ERROR = "error"

class FreshnessResultCompactModel(BaseModel):
    """Компактная модель результата проверки свежести (без вложенных полей)"""
    unique_id: str = Field(..., description="Уникальный идентификатор источника")
    max_loaded_at: datetime = Field(..., description="Максимальное время загрузки данных")
    snapshotted_at: datetime = Field(..., description="Время снятия снэпшота")
    max_loaded_at_time_ago_in_s: float = Field(
        ...,
        description="Время в секундах с момента max_loaded_at (отрицательное - в будущем)"
    )
    status: FreshnessStatus = Field(..., description="Статус проверки: pass/warn/error")
    thread_id: str = Field(..., description="ID потока выполнения")
    execution_time: float = Field(..., description="Общее время выполнения в секундах")


class CloudEvent(BaseModel):
    """
    Pydantic модель для сообщения в формате CloudEvents v1.0.
    """
    # --- ОБЯЗАТЕЛЬНЫЕ ПОЛЯ ---
    specversion: str = Field(default="1.0", description="Версия спецификации CloudEvents. Всегда '1.0'")
    type: str = Field(default="dbt.source.freshness", description="Тип события (например, 'com.example.someevent')")
    source: str = Field(default="dbt-impala", description="URI, идентифицирующий контекст-источник события")
    id: str = Field(..., description="Уникальный идентификатор события, генерируемый источником")

    # --- ОПЦИОНАЛЬНЫЕ ПОЛЯ ---
    time: datetime | None = Field(None, description="Метка времени события в формате RFC3339")

    # --- ПОЛЕ С ДАННЫМИ ---
    data: FreshnessResultCompactModel | None = Field(None, description="Данные, специфичные для события")


def message_process(message: Message) -> None:
    value = message.value()
    logger.info(f'Processing message: {value}\n'
                f'Offset: {message.offset()}')

    match value.data.status:
        case FreshnessStatus.WARN:
            logger.warning(f'[BATCH CONSUMER] ⚠️ Warning for freshness by source: {value.data.unique_id}')
        case FreshnessStatus.ERROR:
            logger.error(f'[BATCH CONSUMER]🚫 Error for freshness by source: {value.data.unique_id}')
        case _:
            pass


running = True

def basic_consume_loop(consumer: DeserializingConsumer | Consumer, topics: t.List[str]):
    """
    Взято из официальной доки https://docs.confluent.io/kafka-clients/python/current/overview.html#basic-poll-loop
    """
    try:
        consumer.subscribe(topics)

        counter = 0
        batch_size = int(os.getenv("MSG_BATCH_SIZE", 10))

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                counter = 0
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # raise KafkaException(msg.error())
                    logger.error(f'Error: {msg.error().code()}')
            else:
                try:
                    message_process(msg)

                    if counter % batch_size == 0:
                        logger.info('Batch commiting...')
                        consumer.commit(asynchronous=False)
                    counter+=1
                except Exception as e:
                    logger.error(f'Error during message processing: {str(e)}')

            logger.info(f'[BATCH CONSUMER] Processed messages: {counter}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False


def main():
    topic = os.getenv("KAFKA_TOPIC", "dev.topic.events.v1")

    # Инициализация клиента Schema Registry
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    json_deserializer = JSONDeserializer(json.dumps(CloudEvent.model_json_schema()),
                                         lambda data, ctx: CloudEvent(**data),
                                         schema_registry_client,)

    consumer_conf.update({
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': json_deserializer,
    })

    with DeserializingConsumer(consumer_conf) as consumer:
        basic_consume_loop(consumer=consumer, topics=[topic])


if __name__ == "__main__":
    main()