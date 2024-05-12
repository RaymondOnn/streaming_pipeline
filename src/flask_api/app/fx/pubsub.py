import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .registry import get_schema_registry_client
from ..utils.settings import KafkaConfig
from ..utils.log import logger

try:
    import confluent_kafka
    from confluent_kafka import KafkaException
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.admin import NewTopic
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField
    from confluent_kafka.serialization import SerializationContext
    from confluent_kafka.serialization import StringSerializer
except ImportError:
    confluent_kafka = None


cfg = KafkaConfig()


class MissingParams(Exception):
    pass


@dataclass
class AvroProducer:
    kafka_brokers: str

    def __post_init__(self) -> None:
        producer_config = {
            "bootstrap.servers": self.kafka_brokers,
            "error_cb": _error_callback_func,
        }
        self.producer = Producer(producer_config)
        logger.info(
            "Created an instance of AvroProducer("
            + ", ".join(f"{k}={v}" for k, v in producer_config.items())
            + ")"
        )

    def set_avro_serializer(self, schema_str: str) -> AvroSerializer:
        """
        Get value serializer from Schema Registry

        This function gets the AvroSerializer from the Schema Registry
        so that it can be used to serialize the event payload.

        Args:
            schema_str (str): Avro Schema in json format

        Returns:
            AvroSerializer: AvroSerializer configured to serialize payload
        """
        sr_client = get_schema_registry_client()
        self.value_serializer = sr_client.make_serializer(schema_str=schema_str)

    def send_message_with_key(
        self,
        topic: str,
        payload: dict,
        key: str,
        poll_timeout_secs: float = 0.5,
    ) -> None:
        """
        Send a message with a key to the specified topic.

        Args:
            topic (str): The name of the topic.
            payload (dict): The event payload to send.
            key (str): The key to send with the message.
            poll_timeout_secs (float, optional): The polling timeout in seconds. Defaults to 0.5.

        Returns:
            None
        """
        key, value = extract_key(payload=payload, key_field_name=key)
        key_serializer = StringSerializer()
        key_bytes = key_serializer(
            key, 
            SerializationContext(topic, MessageField.KEY)
        )
        
        value_bytes = self.value_serializer(
            value, 
            SerializationContext(topic, MessageField.VALUE)
        )
        self.producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=_send_delivery_report,
        )

        # Serve on_delivery callbacks from previous calls to produce()
        self.producer.poll(timeout=poll_timeout_secs)

    def send_message(
        self,
        topic: str,
        schema_str: str,
        payload: dict[str, Any],
        poll_timeout_secs: float = 0.0,
    ) -> None:
        """
        Send a message without a key to the specified topic.

        Args:
            topic (str): The name of the topic.
            schema_str (str): Avro schema in json format
            payload (dict): The event payload to send.
            poll_timeout_secs (float, optional): The polling timeout in seconds. Defaults to 0.0.
        """
        value_bytes = self.value_serializer(
            payload, 
            SerializationContext(topic, MessageField.VALUE)
        )
        self.producer.produce(
            topic=topic,
            value=value_bytes,
            on_delivery=_send_delivery_report,
        )

        # Serve on_delivery callbacks from previous calls to produce()
        self.producer.poll(timeout=poll_timeout_secs)


    def close(self) -> None:
        """Prepare producer for a clean shutdown
        
        Flush pending outbound events, wait for acknowledgement, and process callbacks.
        """
        logger.info("Shutting down..\nFlushing records...")

        # -1 is used to block until all messages are sent.
        self.producer.flush(-1)


def get_avro_producer(kafka_brokers:str=cfg.bootstrap_servers) -> AvroProducer:
    """Create an instance of AvroProducer

    Args:
        kafka_brokers (str, optional): _description_. Defaults to cfg.bootstrap_servers.

    Returns:
        AvroProducer: _description_
    """
    if not confluent_kafka:
        logger.error("Cannot create producer. Confluent-kafka package not available.")

    return AvroProducer(kafka_brokers=kafka_brokers)


def _error_callback_func(kafka_error) -> None:
    """
    A callback function that is called when an error occurs in the Kafka producer.

    Args:
        kafka_error (Exception): The error that occurred in the Kafka producer.

    Raises:
        KafkaException: Raises a KafkaException with the given error.

    Returns:
        None: This function does not return anything.
    """
    logger.error(kafka_error)
    raise KafkaException(kafka_error)


def _send_delivery_report(err, msg) -> None:
    """
    A function that sends a delivery report based on the error and message received.

    Args:
        err: The error that occurred during message delivery.
        msg: The message object containing delivery information.

    Returns:
        None
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
        raise KafkaException(err)

    msg_callback_info = {
        "topic": msg.topic(),
        "topic_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "latency": msg.latency(),
        "produce_kafka_time": datetime.fromtimestamp(
            msg.timestamp()[1] / 1e3,
        ).strftime(
            "%Y-%m-%d %H:%M:%S.%f",
        ),
    }
    logger.info(f"Message sent: {msg_callback_info}")


def extract_key(payload: dict, key_field_name: str) -> tuple[str, dict[str, Any]]:
    """Splits the specified key value from the rest of the dictionary.

    Args:
        payload (dict): Dictionary containing the key value.
        key_field_name (str): Name of key field.

    Returns:
        tuple[str, dict[str, Any]]: A tuple containing the key value and the
        remaining dictionary with the key field removed.

    Raises:
        Exception: If the key field is not found in the payload.
    """
    try:
        key_value = payload.pop(key_field_name)
        if key_value is None:
            raise Exception(f"Failed: Could not find {key_field_name} in payload")
        return str(key_value), payload
    except Exception as e:
        raise Exception(e) from e


def get_admin_client(kafka_brokers: str = cfg.bootstrap_servers) -> AdminClient:
    """
    Create an instance of AdminClient.

    This function creates an instance of AdminClient using the supplied
    list of kafka brokers. If no brokers are provided, it uses the default
    bootstrap servers from the config.

    Args:
        kafka_brokers (str): A comma separated list of kafka brokers.
            Defaults to the value specified in the config.

    Returns:
        AdminClient: An instance of AdminClient.

    Raises:
        Exception: If the confluent_kafka package is not available.
    """
    if not confluent_kafka:
        logger.error(
            "Cannot create producer. Confluent-kafka package not available."
        )

    return AdminClient({"bootstrap.servers": kafka_brokers})


def _topic_exists(client: AdminClient, topic: str) -> bool:
    """Check if a topic exists in Kafka.

    This function uses the AdminClient to retrieve a list of all topics
    from Kafka and then checks if the specified topic is in the list.

    Args:
        client (AdminClient): AdminClient instance
        topic (str): Name of the topic to check for.

    Returns:
        bool: True if the topic exists, False otherwise.
    """
    metadata = client.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    logger.info(f"Topic '{topic}' not found")
    return False



def _confirm_topic_creation(result_dict: dict) -> None:
    """Confirm topic creation was successful

    This function waits for the topic creation futures to complete and
    logs the result of each future. If the topic creation was successful,
    it waits for one second before returning.

    Args:
        result_dict (dict):
            A dict of futures for each topic, keyed by the topic name.
            The future result() method returns None if successful.
    """
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            logger.success(f"Topic '{topic}' successfully created")
            time.sleep(1)
        except Exception as e:
            logger.info(f"Failed to create topic '{topic}': {e}")


def _create_topic(
    client: AdminClient,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Create and registers a new topic in Kafka.

    Creates a new topic with the specified name, number of partitions,
    and replication factor. The topic is created asynchronously,
    and this function waits for the topic creation to complete.

    Args:
        client (AdminClient): AdminClient instance.
        topic (str): Name of the topic to create.
        num_partitions (int, optional):
            Number of partitions to split the topic log into.
            Defaults to 1.
        replication_factor (int, optional):
            Number of backups for each partition.
            Defaults to 1.
    """
    # TODO: # replication_factor limited by number of brokers
    logger.info(
        f"Creating topic '{topic}'" \
        + f" with {num_partitions} partition/s" \
        + f" and replication factor {replication_factor}"
    )
    new_topic = NewTopic(
        topic=topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    # ascertain topic has been created
    results_dict: dict = client.create_topics([new_topic])
    _confirm_topic_creation(results_dict)


def create_topic(client: AdminClient, topic: str) -> None:
    """Creates Kafka topic if it does not exist.

    Args:
        client (AdminClient): AdminClient instance.
        topic (str): Name of the topic to create.
    """
    # probe to check if topic is set up
    if not _topic_exists(client, topic):
        # create topic if not exists
        _create_topic(client, topic)
