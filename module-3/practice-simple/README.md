# practice-simple

Simple-версия practice: **2 брокера Kafka + Schema Registry** и мониторинг через
**Prometheus + Grafana** без JMX.

- `danielqsj/kafka-exporter` — взгляд на кластер со стороны клиента: брокеры,
  партиции/оффсеты топиков, consumer group lag.
- `prom/blackbox-exporter` — пробы доступности: HTTP-корень Schema Registry
  (SR 7.6.0 не имеет `/health`, корень отвечает 200, пока процесс жив) и
  TCP-порты брокеров.

> Host-порты смещены (9097/9098, 8082, 2182, 9091, 3002), чтобы стек можно было
> поднимать одновременно с полным `practice` (9092-9094, 8081, 2181, 9090, 3001).

Топики `topic.events.v1` и `dev.topic.events.v1`: 2 партиции, **RF=2** (создаёт `kafka-init` при старте).

## Запуск

```bash
docker compose up -d
```

> После правки `prometheus/prometheus.yml` или `blackbox/blackbox.yml`:
> `docker compose restart prometheus blackbox-exporter`
> (hot-reload prometheus на bind-mount-файле срабатывает не всегда).

## Порты

| Сервис | Host-порт | Примечание |
|---|---|---|
| zookeeper | 2182 | |
| kafka01 (брокер 1) | 9097 | |
| kafka02 (брокер 2) | 9098 | |
| schema-registry | 8082 | |
| kafka-exporter (danielqsj) | 9308 | метрики kafka_* |
| blackbox-exporter | — | только внутри docker-сети |
| prometheus | 9091 | |
| grafana | 3002 | login `admin`, password `kafka` |

## Проверка

```bash
# брокеры healthy, топики с RF=2
docker compose ps
docker compose exec kafka01 kafka-topics --bootstrap-server kafka01:29092 --describe --topic topic.events.v1
```

## Дашборды (Grafana `:3002`, папка Kafka)

| Дашборд | gnetId | Что показывает |
|---|---|---|
| Kafka Exporter Overview | 7589 | сообщения/сек, lag по consumer group, партиции на топик |
| Blackbox Exporter | 18896 | доступность SR (HTTP) и брокеров (TCP), HTTP-коды, время отклика |

В стеке нет consumer-приложений, поэтому панели consumer group в 7589 пустые —
они заполнятся, когда появится приложение-консьюмер.

## Структура

```
practice-simple/
├── docker-compose.yaml
├── .env                                  # версии образов мониторинга
├── blackbox/
│   └── blackbox.yml                      # модули http_2xx и tcp
├── prometheus/
│   └── prometheus.yml                    # job kafka-exporter + blackbox-http + blackbox-tcp
└── grafana/
    ├── provisioning/
    │   ├── datasources/datasource.yaml   # datasource Prometheus
    │   └── dashboards/provider.yaml      # provider, папка Kafka
    └── dashboards/
        ├── 7589_kafka_exporter.json      # Kafka Exporter Overview
        └── 18896_blackbox_exporter.json  # Blackbox Exporter
```
