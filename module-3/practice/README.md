# practice — Kafka-кластер + мониторинг (JMX → Prometheus → Grafana)

3 брокера + ZooKeeper + Schema Registry + Kafka UI + мониторинг через JMX-экспортёры.

## Состав стека

| Сервис | Порт (host) | Примечание |
|---|---|---|
| zookeeper | 2181 | |
| kafka01 (брокер 1) | 9092 | JMX 9991 — только внутри docker-сети |
| kafka02 (брокер 2) | 9093 | JMX 9992 |
| kafka03 (брокер 3) | 9094 | JMX 9993 |
| schema-registry | 8081 | JMX 9994 |
| kafka-ui | 8080 | |
| jmx-kafka101 / jmx-kafka102 / jmx-kafka103 | 5556 / 5557 / 5558 | `sscaling/jmx-prometheus-exporter` |
| jmx-schema-registry | 5559 | тот же экспортёр, JVM-метрики SR |
| prometheus | 9090 | |
| alertmanager | 9095 | (9093 занят брокером kafka02) |
| grafana | 3001 | login `admin`, password `kafka` |

Топики `topic.events.v1` и `dev.topic.events.v1`: 3 партиции, **RF=3** (создаются `kafka-init` при старте).

## Запуск

```bash
docker compose -f docker-compose.kafka.yaml down -v
docker compose -f docker-compose.kafka.yaml up -d
```


## Проверка

```bash
docker compose -f docker-compose.kafka.yaml ps
```

Grafana: http://localhost:3001 → папка **Kafka** → 8 дашбордов (Healthcheck, Replication, Performance, JVM/OS, Topics/Logs, Zookeeper, …).

## Алертинг

- Правила: `prometheus/alert.rules` — `monitor_service_down` (up == 0), `kafka_broker_not_stable` (brokerstate != 3), `kafka_offline_partitions`, `kafka_under_replicated_partitions` (5m, warning).
- Alertmanager: `:9095` (UI: Alerts/Status; правила: `:9090/rules`). Роутинг — `prometheus/alertmanager.yml`: receiver `mattermost`, в `url:` вписать URL вебхука и выполнить `docker compose -f docker-compose.kafka.yaml restart alertmanager`.

## Мониторинг: почему JMX-экспортёр

- `sscaling/jmx-prometheus-exporter` читает **собственные JMX MBean'ы каждого брокера** — авторитетное состояние: per-broker throughput, p75/p99 латентности Produce/Fetch, ISR shrink/expand, under-replicated, fetcher lag, controller state, JVM/GC/heap.
- Универсальный: один инструмент на любой Java-сервис (Kafka + Schema Registry).
- Цена: на брокере должен быть включён JMX (`KAFKA_JMX_PORT`) и сеть до JMX-порта.

Альтернатива `danielqsj/kafka-exporter` меряет кластер **со стороны клиента** (metadata + offsets): удобен consumer group lag по группам приложений, но нет JVM/OS, percentiles и per-broker разбивки, и SR он не мониторит. 


### Важно про Schema Registry

> **SR в этом стеке доступен для мониторинга только в режиме JMX без аутентификации** — поэтому `cp-schema-registry` запинен на `7.6.0` (там JMX включается одним env `SCHEMA_REGISTRY_JMX_PORT`). 
> В 8.x удалённый JMX включается только с обязательной парольной JMX-аутентификацией, а `sscaling/jmx-prometheus-exporter` JMX-пароли не умеет, так что на 8.x экспортёр к SR не подключится. 
> SR не публикует предметных JMX MBean'ов (метрики REST-апика — через Spring Boot Actuator), поэтому по нему идут JVM/OS-метрики (`kafka_jvm_*`) — он виден в дашборде JVM/OS как отдельный instance.

## Структура конфигов

```
practice/
├── docker-compose.kafka.yaml
├── .env                                  # версии Grafana/Prometheus, heap экспортёра
├── template/
│   └── jmx_exporter_kafka_broker.yml     # шаблон конфига брокера (плейсхолдер %HOST_PORT%)
├── jmx_exporter/
│   ├── config_kafka101.yml               # hostPort: kafka01:9991
│   ├── config_kafka102.yml               # hostPort: kafka02:9992
│   ├── config_kafka103.yml               # hostPort: kafka03:9993
│   └── config_schema-registry.yml        # hostPort: schema-registry:9994 (только java.lang)
├── prometheus/
│   ├── prometheus.yml                    # job kafka-jmx: 4 таргета + self, rule_files, alerting
│   ├── alert.rules                       # правила алертов
│   └── alertmanager.yml                  # роутинг → mattermost-webhook (url вписать)
└── grafana/
    ├── provisioning/
    │   ├── datasources/datasource.yaml   # datasource Prometheus
    │   └── dashboards/provider.yaml      # provider, папка Kafka
    └── dashboards/                       # 8 JSON-дашбордов под JMX-метрики
```

Добавить брокер: сервис в compose (id, порт, `KAFKA_JMX_PORT`), bootstrap-серверы в SR/kafka-init/kafka-ui, экспортёр + конфиг из `template/`, таргет в `prometheus.yml`.

## Проверка

### producer
```bash
docker exec -it kafka-course-01 \
kafka-producer-perf-test \
--throughput 500 \
--num-records 100000000 \
--topic dev.topic.events.v1 \
--record-size 100 \
--producer-props bootstrap.servers=kafka-course-01:29092
```

### consumer

```bash
docker exec -it kafka-course-01 \
kafka-consumer-perf-test \
--messages 100000000 \
--timeout 1000000 \
--topic dev.topic.events.v1 \
--reporting-interval 1000 \
--show-detailed-stats \
--bootstrap-server kafka-course-01:29092
```