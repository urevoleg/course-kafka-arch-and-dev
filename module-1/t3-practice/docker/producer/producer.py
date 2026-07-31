import json
import logging
import os
import random
import time
from datetime import datetime
from enum import Enum

import pendulum
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


producer_config = {
   'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093"),
    "acks": 1,  # at least once, ждем подтверждение от 1 реплики
}


schema_registry_config = {
   'url': os.getenv("SCHEMA_REGISTRY_URL", 'http://localhost:8081')
}


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


SOURCES = [
    "source.dwh.core.v_customers",
    "source.dwh.core.v_products",
    "source.dwh.core.v_orders",
    "source.dwh.core.v_suppliers",
    "source.dwh.core.v_employees",
    "source.dwh.transactions.v_sales",
    "source.dwh.transactions.v_payments",
    "source.dwh.transactions.v_refunds",
    "source.dwh.transactions.v_invoices",
    "source.dwh.inventory.v_stock_levels",
    "source.dwh.inventory.v_warehouse_movements",
    "source.dwh.inventory.v_reorder_points",
    "source.dwh.analytics.v_daily_kpis",
    "source.dwh.analytics.v_monthly_aggregates",
    "source.dwh.analytics.v_top_products",
    "source.dwh.analytics.v_customer_segments",
]

STATUSES = [FreshnessStatus.PASS, FreshnessStatus.WARN, FreshnessStatus.ERROR]


def generate_random_freshness_event() -> CloudEvent:
    """Генерирует случайное CloudEvent сообщение о свежести данных"""

    source = random.choice(SOURCES)
    status = random.choices(
        STATUSES,
        weights=[0.7, 0.2, 0.1]  # 70% pass, 20% warn, 10% error
    )[0]

    now = pendulum.now(tz="local")
    max_loaded_at = now.replace(second=0, microsecond=0).subtract(seconds=random.randint(-1800, 3600))
    snapshotted_at = now

    # Вычисляем разницу в секундах
    max_loaded_at_time_ago_in_s = (max_loaded_at - now).total_seconds()

    # Создаем данные
    data = FreshnessResultCompactModel(
        unique_id=source,
        max_loaded_at=max_loaded_at,
        snapshotted_at=snapshotted_at,
        max_loaded_at_time_ago_in_s=max_loaded_at_time_ago_in_s,
        status=status.value,
        thread_id="Thread-4 (worker)",
        execution_time=round(random.uniform(0.5, 5.0), 3)
    )

    # Создаем CloudEvent
    return CloudEvent(
        id=str(int(pendulum.now().timestamp())),
        type="dbt.source.freshness.v1",
        source="dbt.freshness",
        data=data,
        time=pendulum.now(tz="local")
    )


def main():
    topic = os.getenv("KAFKA_TOPIC", "dev.topic.events.v1")

    # Инициализация клиента Schema Registry
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    # Создание JSON-сериализатора
    json_serializer = JSONSerializer(json.dumps(CloudEvent.model_json_schema()), schema_registry_client)

    # Инициализация продюсера
    producer = Producer(producer_config)

    # Сериализация ключа и значения
    key_serializer = StringSerializer('utf_8')
    value_serializer = json_serializer

    # Функция обратного вызова для подтверждения доставки
    def delivery_report(err, msg):
       if err is not None:
           logger.error(f"Message delivery failed: {err}")
       else:
           logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    while True:
        if time.time() % 10 != 0:
            continue

        # доходим сюда раз в 10 секунд -  для тестов достаточно
        # Отправка сообщения

        # Сообщение для отправки
        message = generate_random_freshness_event()

        try:
            producer.produce(
               topic=topic,
               key=key_serializer(message.id, SerializationContext(topic, MessageField.VALUE)),
               value=value_serializer(message.model_dump(mode="json"), SerializationContext(topic, MessageField.VALUE)),
               on_delivery=delivery_report
            )

            logger.info(f'Send: {message.model_dump()}')
        except Exception as e:
            logger.error(str(e))
        finally:
            # Очистка очереди сообщений
            producer.flush()



if __name__ == "__main__":
    main()