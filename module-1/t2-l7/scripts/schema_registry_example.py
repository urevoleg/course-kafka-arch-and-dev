import os

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from pydantic import BaseModel, Field

from dotenv import load_dotenv
load_dotenv()


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


def main():



# Инициализация клиента Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_config)


# Создание JSON-сериализатора
json_serializer = JSONSerializer(json_schema_str, schema_registry_client)


# Инициализация продюсера
producer = Producer(kafka_config)


# Тема Kafka
topic = 'your-topic'


# Сообщение для отправки
message_value = {"id": 30, "name": "product"}


# Сериализация ключа и значения
key_serializer = StringSerializer('utf_8')
value_serializer = json_serializer


# Функция обратного вызова для подтверждения доставки
def delivery_report(err, msg):
   if err is not None:
       print(f"Message delivery failed: {err}")
   else:
       print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Отправка сообщения
producer.produce(
   topic=topic,
   key=key_serializer("user_key", SerializationContext(topic, MessageField.VALUE)),
   value=value_serializer(message_value, SerializationContext(topic, MessageField.VALUE)),
   on_delivery=delivery_report
)


# Очистка очереди сообщений
producer.flush()