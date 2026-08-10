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

INPUT_KAFKA_TOPIC = os.getenv("INPUT_KAFKA_TOPIC", "input.topic.events.v1")
OUTPUT_KAFKA_TOPIC = os.getenv("OUTPUT_KAFKA_TOPIC", "output.topic.events.v1")


def _raise_if_not_env_value(env_name: str):
    value = os.getenv(env_name, None)
    if value is None:
        raise ValueError(f'Env: {env_name} is not set!')
    return value


app = faust.App(
        "example-faust-app",
        broker=_raise_if_not_env_value("KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer="raw"
)

input_topic = app.topic(INPUT_KAFKA_TOPIC, key_type=str, value_type=str)
output_topic = app.topic(OUTPUT_KAFKA_TOPIC, key_type=str, value_type=str)

@app.agent(input_topic)
async def process(stream):
    async for value in stream:
        # Обработка данных
        processed_value = f"Processed: {value}"
        # Отправка обработанных данных в выходной топик
        await output_topic.send(value=processed_value)

