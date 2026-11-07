# Балансировка партиций и инструменты администрирования

> ДИСКЛЕйМЕР: в образе image: confluentinc/cp-kafka:7.6.0 (и других Confluent) доступны kafka-topics и kafka-consumer-groups


Утилиты:
- `kafka-topics`
- `kafka-leader-election`
- `kafka-reassign-partitions`
- `kafka-console-consumer`
- `kafka-console-producer`

best practice cli:

```bash
kafka-topics --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --list

kafka-topics --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --describe --topic dev.topic.events.v1

kafka-topics --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --alter --topic dev.topic.events.v1 --partitions 8

kafka-topics --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --describe --under-replicated-partitions

kafka-leader-election --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --election-type PREFERRED --all-topic-partitions
```

## kafka-reassign-partitions

```bash
echo '{
  "version": 1,
  "partitions": [
    {"topic": "dev.topic.events.v1", "partition": 0, "replicas": [1, 2], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 1, "replicas": [3, 1], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 2, "replicas": [2, 3], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 3, "replicas": [1, 2], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 4, "replicas": [3, 1], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 5, "replicas": [2, 3], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 6, "replicas": [1, 2], "log_dirs": ["any", "any"]},
    {"topic": "dev.topic.events.v1", "partition": 7, "replicas": [3, 1], "log_dirs": ["any", "any"]}
  ]
}' > /tmp/reassignment.json
```


```bash
kafka-reassign-partitions \
--bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 \
--broker-list "1,2" \
--topics-to-move-json-file "/tmp/reassignment.json" \
--generate


kafka-reassign-partitions --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --reassignment-json-file /tmp/reassignment.json --execute
```

### consumer groups


```bash
kafka-consumer-groups --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --list

kafka-console-consumer --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --topic dev.topic.events.v1
```

### kafka-console-consumer

Часто используемые параметры:
- `--from-beginning` — читать сообщения с самого начала (включая старые, уже обработанные).
- `--max-messages <N>` — считать только N сообщений, затем завершить работу. Полезно в тестах.
- `--partition <N>` — читать только из одной партиции.
- `--formatter <CLASSNAME>` — форматировать вывод. По умолчанию — `kafka.tools.DefaultMessageFormatter`. 
Можно использовать, например, для вывода ключа, значения и метаданных сообщений.

Пример:
```bash
kafka-console-consumer \
  --bootstrap-server kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 \
  --topic dev.topic.events.v1 \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=" => "
```

### kafka-console-producer

Полезные параметры:
- `--broker-list`
- `--property parse.key=true` — позволяет указывать ключ сообщения.
- `--property key.separator=:` — определяет символ-разделитель между ключом и значением (по умолчанию — табуляция).
- `--producer-property acks=all` — установка параметров продюсера, как в обычной Java-конфигурации.

Пример:
```bash
kafka-console-producer --broker-list kafka:29092,kafka-broker-2:29093,kafka-broker-3:29094 --topic input.topic.events.v1 \
--property parse.key=true --property key.separator=:
```

-----------

⏩ [Main](https://github.com/urevoleg/course-kafka-arch-and-dev)

⏩ [Current module](https://github.com/urevoleg/course-kafka-arch-and-dev/tree/main/module-3)