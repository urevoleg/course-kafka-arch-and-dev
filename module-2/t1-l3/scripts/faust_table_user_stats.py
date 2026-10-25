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
        broker=_raise_if_not_env_value("KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer="raw"
)


# create tables
user_stats_table = app.Table(name="user_stats",
                             default=UserStats,
                             partitions=3,
                             help="Статистика по пользователям")

user_windowed_stats_table = app.Table(name="windowed_stats",
                             default=UserStats,
                             partitions=3,
                             help="Статистика по пользователям")\
    .hopping(size=timedelta(seconds=30),
             step=timedelta(seconds=10),
             expires=timedelta(hours=1),
             key_index=True)

user_events_topic = app.topic(INPUT_KAFKA_TOPIC, value_type=UserEvent)

@app.agent(user_events_topic)
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


       # Обновляем статистику по временному окну
       window = user_windowed_stats_table[event.user_id].now()
       if window is None:
           window = UserStats()
       window.total_events += 1
       window.last_event_time = event.timestamp
       window.total_value += event.value
       user_windowed_stats_table[event.user_id] = window


@app.page('/stats/{user_id}')
async def get_user_stats(web, request, user_id):
   try:
       stats = user_stats_table[user_id]
       if stats is None:
           return web.json({
               'user_id': user_id,
               'total_events': 0,
               'last_event_time': datetime.now(),
               'total_value': 0.0
           })
       return web.json({
           'user_id': user_id,
           'total_events': stats.total_events,
           'last_event_time': stats.last_event_time,
           'total_value': stats.total_value
       })
   except Exception as e:
       return web.json({
           'error': str(e),
           'user_id': user_id
       }, status=500)


@app.page('/windowed_stats/{user_id}')
async def get_windowed_stats(web, request, user_id):
   try:
       window = windowed_stats[user_id].now()
       if window is None:
           return web.json({
               'user_id': user_id,
               'total_events': 0,
               'last_event_time': datetime.now(),
               'total_value': 0.0
           })


       return web.json({
           'user_id': user_id,
           'total_events': window.total_events,
           'last_event_time': window.last_event_time,
           'total_value': window.total_value
       })
   except Exception as e:
       return web.json({
           'error': str(e),
           'user_id': user_id
       }, status=500)