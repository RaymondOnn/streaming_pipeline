import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.serialization import Serializer


def _error_callback_func(kafka_error) -> None:
    logging.error(kafka_error)
    raise KafkaException(kafka_error)


def _send_delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
        raise KafkaException(err)

    msg_callback_info = {
        "latency": msg.latency(),
        "kafka_offset": msg.offset(),
        "topic_partition": msg.partition(),
        "data_tag": msg.topic(),
        "produce_kafka_time": datetime.fromtimestamp(
            msg.timestamp()[1] / 1e3,
        ).strftime(
            "%Y-%m-%d %H:%M:%S.%f",
        ),
    }
    logging.info(f"Message sent: {msg_callback_info}")


class MissingParams(Exception):
    pass


@dataclass
class AvroProducer:
    kafka_brokers: str
    value_serializer: AvroSerializer
    key_serializer: Serializer | None = None

    def __post_init__(self):
        producer_config = {
            "bootstrap.servers": self.kafka_brokers,
            "error_cb": _error_callback_func,
        }
        self.producer = Producer(producer_config)
        logging.info(
            "Created an instance of AvroProducer("
            + ", ".join(f"{k}={v}" for k, v in producer_config.items())
            + ")"
        )

    def send_message_with_key(
        self,
        topic: str,
        payload: dict,
        key: str,
        poll_timeout_secs: float = 0.5,
    ):
        # Serve on_delivery callbacks from previous calls to produce()
        self.producer.poll(timeout=poll_timeout_secs)

        # check if key_Serializer is assigned
        if self.key_serializer is None:
            raise MissingParams(
                "Parameter 'key' is required to publish messages with a key"
            )

        else:
            key = payload.pop(key)
            key_ctx = SerializationContext(topic, MessageField.KEY)
            val_ctx = SerializationContext(topic, MessageField.VALUE)
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(key, key_ctx),
                value=self.value_serializer(payload, val_ctx),
                on_delivery=_send_delivery_report,
            )

        # print("\nFlushing records...")
        self.producer.flush()

    def send_message(
        self,
        topic: str,
        payload: dict[str, Any],
        poll_timeout_secs: float = 0.0,
    ):
        # Serve on_delivery callbacks from previous calls to produce()
        self.producer.poll(timeout=poll_timeout_secs)

        val_ctx = SerializationContext(topic, MessageField.VALUE)
        self.producer.produce(
            topic=topic,
            value=self.value_serializer(payload, val_ctx),
            on_delivery=_send_delivery_report,
        )

        # print("\nFlushing records...")
        self.producer.flush()


# return True if topic exists and False if not
def _topic_exists(client: AdminClient, topic: str) -> bool:
    metadata = client.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    print(f"Topic '{topic}' not found")
    return False


def _confirm_topic_creation(result_dict):
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print(f"Topic '{topic}'succesfully created")
            time.sleep(1)
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")


def _create_topic(
    client: AdminClient,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    # TODO: # replication_factor limited by number of brokers
    print(
        f"Creating topic '{topic}' \
            with {num_partitions} partition/s \
            and replication factor {replication_factor}"
    )
    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    # ascertain topic has been created
    results_dict = client.create_topics([new_topic])
    _confirm_topic_creation(results_dict)


def create_topic(client: AdminClient, topic: str):
    # probe to check if topic is set up
    if not _topic_exists(client, topic):
        _create_topic(client, topic)
