# Потоковая обработка данных: ksql, ksqlDB

- **ksql** - интерфейс для Kafka, чтобы выполнять простые действия
- **ksqlDB** - дальнейшее развитие [ksql](https://www.confluent.io/blog/intro-to-ksqldb-sql-database-streaming/), 
основанное на клиент-серверном подходе, позволяет решать более сложные задачи. Стало системой для потоковой обработки данных, работате на основе KafkaStreams

## Недостатки

- есть ограничения на SQL операции, например, не поддерживаются подзапросы, что может быть критичным при реализации некоторой бизнес-логики
- есть накладные расходы (тк ksqlDB обертка над KafkaStreams, то есть небольшие накладные расходы на взаимодействие)


## Основные компоненты ksqlDB

![](../../img/m2-t1-l5-ksql-components.png)


## Режимы работы

- **adhoc** - обычные SQL запросы, без сохранения состояния, возращают результат сразу пользователю в клиент

```sql
-- Пример интерактивного запроса
SELECT customer_id, product_name, quantity, price
FROM orders_stream
WHERE price > 100
EMIT CHANGES LIMIT 10;
```


```bash
curl --location 'http://localhost:8088/query' \
--header 'Content-Type: application/vnd.ksql.v1+json' \
--data '{
  "ksql": "SELECT customer_id, product_name, quantity, price FROM orders_stream WHERE price > 100 EMIT CHANGES LIMIT 10;"
}'
```

- **persistent queries** - постоянные запросы, которые обновляют данные, работают в фоновом режиме, записывают данные в топики кафка

```sql
-- Пример постоянного запроса
CREATE STREAM high_value_orders AS
    SELECT order_id, customer_id, product_name, price
    FROM orders_stream
    WHERE price > 1000
EMIT CHANGES;
```

## kafka-ui


Для выполнения запросов ksql из UI, необходимо её подключить в конфигурации:

```yaml
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui-course
    environment:
      ....
      KAFKA_CLUSTERS_0_KSQLDBSERVER: http://ksqldb-server:8088 # для подключения ksqlDB в UI
```


![](../../img/m2-t1-l5-ksql-kafka-ui.png)


