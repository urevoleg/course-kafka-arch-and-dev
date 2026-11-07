import logging
import os
import re
from datetime import datetime

import faust
from dotenv import load_dotenv

import pendulum

load_dotenv()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# Контракты сообщений (записи)

class Message(faust.Record):
    """
    Сообщение чата (входящее и исходящее).

    {
      "message_id": 1,
      "from_user_id": "user_01",
      "to_user_id": "user_02",
      "text": "hello user_02",
      "timestamp": "2026-11-07T10:00:00"
    }
    """
    message_id: int
    from_user_id: str
    to_user_id: str
    text: str
    timestamp: datetime


class BlockEvent(faust.Record):
    """
    Событие блокировки/разблокировки пользователя.
    Запись несёт желаемое состояние (is_blocked), поэтому повторная
    отправка одного и того же события идемпотентна.

    {
      "from_user_id": "user_02",
      "blocked_user_id": "user_01",
      "is_blocked": 1,
      "timestamp": "2026-11-01T00:45:37"
    }
    """
    from_user_id: str
    blocked_user_id: str
    is_blocked: int  # 1 - заблокировать, 0 - разблокировать
    timestamp: datetime = pendulum.now(tz="local")


class BannedWordEvent(faust.Record):
    """
    Событие бана/разбана слова. Тот же паттерн состояния, что у BlockEvent.

    {
      "word": "spam",
      "is_banned": 1,
      "timestamp": "2026-11-01T00:45:37"
    }
    """
    word: str
    is_banned: int  # 1 - забанить, 0 - разбанить
    timestamp: datetime = pendulum.now(tz="local")


app = faust.App(
    "message-processor",
    broker=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    store="rocksdb://",
)


# Таблицы состояния (persistent state store)

# Списки заблокированных: key = пользователь-блокировщик,
# value = список заблокированных user_id.
# value_type не задаём: встроенные типы (list) не имеют from_data() и
# ломают десериализацию changelog, без value_type значения ходят как JSON
blocked_table = app.Table(
    name="blocked",
    default=list,
    key_type=str,
    partitions=3,
    help="Списки заблокированных пользователей по пользователям",
)

# Запрещённые слова: key = слово, value = is_banned (0/1, JSON)
banned_words_table = app.Table(
    name="banned_words",
    default=int,
    key_type=str,
    partitions=3,
    help="Список запрещённых слов (цензура)",
)


# Топики

messages_topic = app.topic("messages", key_type=str, value_type=Message)
filtered_messages_topic = app.topic("filtered_messages", key_type=str, value_type=Message)
blocked_users_topic = app.topic("blocked_users", key_type=str, value_type=BlockEvent)
banned_words_topic = app.topic("banned_words", key_type=str, value_type=BannedWordEvent)


# Цензура

def censor(text: str, banned: list) -> str:
    """
    Грубая цензура: маскирует вхождения запрещённых слов по всему тексту.

    'buy cheap SPAM now' -> 'buy cheap **** now'
    'You are spammer!' -> 'You are ****mer!''

    Регистр не учитываем, длина маски равна длине вхождения.
    Варианты улучшений - в README.
    """
    for word in banned:
        regex = re.compile(re.escape(word), re.IGNORECASE)
        text = regex.sub(lambda m: "*" * len(m.group()), text)
    return text


# Агенты

@app.agent(blocked_users_topic)
async def process_block_events(stream):
    """Поддерживает списки заблокированных пользователей"""
    async for event in stream:
        blocked_list = blocked_table[event.from_user_id] or []

        # обновление поюзерных black-list
        if event.is_blocked:
            if event.blocked_user_id not in blocked_list:
                blocked_list.append(event.blocked_user_id)
        else:
            if event.blocked_user_id in blocked_list:
                blocked_list.remove(event.blocked_user_id)

        # перезаписываем значение - таблица синхронизируется в changelog

        blocked_table[event.from_user_id] = blocked_list
        logger.info(
            f"Blocked list of {event.from_user_id} "
            f"after event (is_blocked={event.is_blocked} for {event.blocked_user_id}): "
            f"{blocked_list}"
        )


@app.agent(banned_words_topic)
async def process_banned_words(stream):
    """Поддерживает список запрещённых слов"""
    async for event in stream:
        banned_words_table[event.word] = event.is_banned
        logger.info(f"Banned word {event.word!r}: is_banned={event.is_banned}")


@app.agent(messages_topic)
async def process_messages(stream):
    """
    Конвейер обработки сообщения:
    1) проверка блокировки - получатель заблокировал отправителя?
    2) цензура - маскируем запрещённые слова
    3) пересылка в топик filtered_messages
    """

    async for message in stream:
        # 1) Блокировка: получатель (to_user) заблокировал отправителя (from_user)?
        blocked_list = blocked_table[message.to_user_id] or []
        if message.from_user_id in blocked_list:
            logger.warning(
                f"[BLOCKED] Message {message.message_id} from {message.from_user_id} "
                f"to {message.to_user_id} dropped: sender is blocked"
            )
            continue

        # 2) Цензура: текущий список запрещённых слов из таблицы
        banned = [word for word, is_banned in banned_words_table.items() if is_banned]
        censored_text = censor(message.text, banned)

        # 3) Отправка в выходной топик
        filtered_message = Message(
            message_id=message.message_id,
            from_user_id=message.from_user_id,
            to_user_id=message.to_user_id,
            text=censored_text,
            timestamp=message.timestamp,
        )
        await filtered_messages_topic.send(value=filtered_message, key=message.to_user_id)

        if censored_text != message.text:
            logger.info(
                f"[CENSOR] Message {message.message_id} censored: "
                f"{message.text!r} -> {censored_text!r}"
            )
        else:
            logger.info(f"Message {message.message_id} delivered: {censored_text!r}")


if __name__ == "__main__":
    # Локальный запуск: python3 message_processor.py
    # В этой версии faust-streaming worker стартует через CLI (подкоманда worker),
    # app.main() без аргументов печатает help и завершается
    # import sys
    #
    # sys.argv = [sys.argv[0], "worker"]
    # from faust.cli.faust import cli
    #
    # cli(app=app)
    app.main()
