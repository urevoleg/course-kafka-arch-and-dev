# Топики

Создание топиков можно выполнить через:
- консоль, зайдя в докер контейнер

```bash
docker exec <container-name> /bin/bash

kafka-topics --create --topic <topic-name> --partitions 1 --replication-factor 1  --bootstrap-server kafka:9092

-- проверка
kafka-topics --list --bootstrap-server kafka:9092
```

- python скрипт

пример тут [create_topic.py](scripts/create_topic.py)