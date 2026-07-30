from kafka import KafkaAdminClient

from dotenv import load_dotenv
load_dotenv()

"""{topic_name: {num_partitions: int (default -1),
              replication_factor: int (default -1),
              assignments: {partition_id: [broker_ids]}"""
default_topic_config = {
    "num_partitions": 1,
    "replication_factor": 1
}


def main():
    kafka_admin = KafkaAdminClient(bootstrap_servers="http://127.0.0.1:9092")
    kafka_admin.create_topics({
        "second_topic": default_topic_config
    })


if __name__ == "__main__":
    main()