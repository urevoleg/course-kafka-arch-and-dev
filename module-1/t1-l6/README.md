# Установка и настройка Kafka кластеров

1. Zookeeper

Проблем нет, установилось: [docker-compose.kafka.yaml](../../infra/docker-compose.kafka.yaml)

2. Kraft - mode

с заготовкой из курса получил ошибку:

```
Failed to write meta.properties due to (kafka.server.BrokerMetadataCheckpoint)
```


stackoverflow в помощь [link](https://stackoverflow.com/questions/67317088/kafka-error-failed-to-write-meta-properties-due-to-kafka-server-brokermetadat), 
сменил версию, добавил ui, упало с ошибкой

```
/opt/bitnami/scripts/libkafka.sh: line 242: KAFKA_CFG_BROKER_ID: unbound variable
```

Докинул переменную

```yaml
    environment:
      - ...
      - KAFKA_CFG_BROKER_ID=2
```

Кафка всё еще падает без логов

```
kafka-course-kraft  | kafka 09:44:35.05 WARN  ==> KAFKA_CFG_BROKER_ID Must match what is set in KAFKA_CFG_CONTROLLER_QUORUM_VOTERS
kafka-course-kraft  | kafka 09:44:35.06 WARN  ==> KAFKA_CFG_CONTROLLER_QUORUM_VOTERS must match brokers set with KAFKA_CFG_BROKER_ID
kafka-course-kraft  | kafka 09:44:35.09 WARN  ==> KAFKA_CFG_PROCESS_ROLES must include 'controller' for KRaft
kafka-course-kraft  | kafka 09:44:35.11 WARN  ==> KAFKA_CFG_LISTENERS must include a listener for CONTROLLER
kafka-course-kraft  | kafka 09:44:35.16 WARN  ==> You set the environment variable ALLOW_PLAINTEXT_LISTENER=yes. For safety reasons, do not use this flag in a production environment.
kafka-course-kraft  | kafka 09:44:35.25 INFO  ==> Initializing Kafka...
kafka-course-kraft  | kafka 09:44:35.31 INFO  ==> No injected configuration files found, creating default config files
kafka-course-kraft  | kafka 09:44:36.73 INFO  ==> Configuring Kafka for external client communications with PLAINTEXT authentication.
kafka-course-kraft  | kafka 09:44:36.75 WARN  ==> External client communications are configured using PLAINTEXT listeners. For safety reasons, do not use this in a production environment.
kafka-course-kraft  | kafka 09:44:36.83 INFO  ==> Initializing KRaft...
kafka-course-kraft  | kafka 09:44:36.84 INFO  ==> Formatting storage directories to add metadata...
kafka-course-kraft exited with code 1  
```



-----------

⏩ [Main](https://github.com/urevoleg/course-kafka-arch-and-dev)