import os
import logging
import json

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from pydantic import BaseModel, Field

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# Конфигурация для Kafka и Schema Registry
kafka_config = {
   'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
}


schema_registry_config = {
   'url': os.getenv("SCHEMA_REGISTRY_URL", 'http://localhost:8081')
}


class Product(BaseModel):
    id: int = Field(description="product ID")
    name: str = Field(description="Наименование")
    description: str | None = Field(default="", description="Описание товара")


def main():
    topic = "topic.events.v1"

    # Инициализация клиента Schema Registry
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    # Создание JSON-сериализатора
    json_serializer = JSONSerializer(json.dumps(Product.model_json_schema()), schema_registry_client)

    # Инициализация продюсера
    producer = Producer(kafka_config)

    # Сообщение для отправки
    message_value = Product(id=30, name="product").model_dump()

    # Сериализация ключа и значения
    key_serializer = StringSerializer('utf_8')
    value_serializer = json_serializer

    # Функция обратного вызова для подтверждения доставки
    def delivery_report(err, msg):
       if err is not None:
           logger.error(f"Message delivery failed: {err}")
       else:
           logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


    # Отправка сообщения
    producer.produce(
       topic=topic,
       key=key_serializer("user_key", SerializationContext(topic, MessageField.VALUE)),
       value=value_serializer(message_value, SerializationContext(topic, MessageField.VALUE)),
       on_delivery=delivery_report
    )

    # Очистка очереди сообщений
    producer.flush()


if __name__ == "__main__":
    main()