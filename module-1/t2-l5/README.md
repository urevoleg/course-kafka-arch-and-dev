# Настройка репликации через код

1. Перешел на confluent_kafka
2. Создать топик с дефисами не получается и ошибок нет
3. Создать топик с replication_factor>1, если реплика только 1, не получается, но и ошибок нет
4. Создать топик с конфигом

```python
    topic = NewTopic(topic=topic_name,
                     num_partitions=1,
                     replication_factor=1,
                     config={"min.insync.replicas": "2"}
                     )
```

Можно, но отправить сообщение нельзя


## Добавление брокера

Для проверки п3-4 добавил еще брокер, конфиг [docker-compose.kafka-add-broker.yaml](../../infra/docker-compose.kafka-add-broker.yaml)

Топики с репликацией добавляются успешно:

![](../../img/t2-l5-two-brokers.png)