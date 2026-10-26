import json
from datetime import datetime, timedelta
import os
import logging

from dotenv import load_dotenv
import faust

load_dotenv()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

INPUT_KAFKA_TOPIC = os.getenv("INPUT_KAFKA_TOPIC_USER_EVENTS", "user_events")


class UserEvent(faust.Record):
    """
    {
    "user_id": "user123",
    "event_type": "purchase",
    "timestamp": "2024-03-20T15:30:45.123456",
    "value": 149.99
    }
    """
    user_id: str
    event_type: str
    timestamp: datetime
    value: float


class UserStats(faust.Record):
   total_events: int = 0
   last_event_time: datetime = datetime.now()
   total_value: float = 0.0


def _raise_if_not_env_value(env_name: str):
    value = os.getenv(env_name, None)
    if value is None:
        raise ValueError(f'Env: {env_name} is not set!')
    return value


app = faust.App(
        "table_user_stats",
        broker=_raise_if_not_env_value("KAFKA_BOOTSTRAP_SERVERS")
)


# create tables
user_stats_table = app.Table(name="user_stats",
                             default=UserStats,
                             partitions=3,
                             help="Статистика по пользователям")


user_events_topic = app.topic(INPUT_KAFKA_TOPIC, value_type=UserEvent)
user_stats_topic = app.topic('user_stats_output', value_type=UserStats)

@app.agent(user_events_topic, sink=[user_stats_topic])
async def process(events):
    async for event in events:
        # Обновляем общую статистику
        stats = user_stats_table[event.user_id]
        if stats is None:
           stats = UserStats()
        stats.total_events += 1
        stats.last_event_time = event.timestamp
        stats.total_value += event.value
        user_stats_table[event.user_id] = stats

        if stats.total_value > 100:
            yield stats


if __name__ == "__main__":
    app.main()