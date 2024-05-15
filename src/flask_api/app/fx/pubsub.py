import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Self, Type

from .registry import get_schema_registry_client, make_schema_str
from ..utils.settings import KafkaConfig
from ..utils.log import logger

try:
    import confluent_kafka
    from confluent_kafka import KafkaException
    from confluent_kafka import Producer
    from confluent_kafka import Message
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
        logger.debug(
            "Created an instance of AvroProducer("
            + ", ".join(f"{k}={v}" for k, v in producer_config.items())
            + f"): {self.producer}",
        )

    def set_avro_serializer(
        self,
        schema_str: Optional[str] = None,
        class_object: Optional[Type] = None,
    ) -> "AvroProducer":
        """
        Get value serializer from Schema Registry

        This function gets the AvroSerializer from the Schema Registry
        and assigns it as the value_serializer to be used for serializing
        the event payload.

        Args:
            schema_str (str, optional): Avro Schema in json format.
            class_object (Type, optional): Any dataclass or pydantic model
            NOTE: Either `schema_str` or `class_object` must be provided.

        Returns:
            AvroProducer: Returns self to allow chaining of method calls.

        Raises:
            Exception: If both `schema_str` and `class_object` are provided,
                or if neither is provided.
        """
        schema_string = "{}"
        
        if schema_str is not None and class_object is not None:
            raise Exception(
                "Cannot pass both 'schema_str' and 'class_object'"
            )

        if schema_str is None and class_object is None:
            raise Exception("Must pass either 'schema_str' or 'class_object'")

        if class_object is not None:
            schema_string = make_schema_str(class_object)
            
            
        try:
            sr_client = get_schema_registry_client()
            self.value_serializer = sr_client.make_serializer(schema_str=schema_string)
            logger.success("AvroSerializer set. AvroProducer ready to send messages.")
        except Exception as e:  
            logger.exception(e)
            raise Exception(f"Failed to set AvroSerializer: {e}")
        finally:
            return self

    def send_message_with_key(
        self,
        topic: str,
        payload: dict,
        key: str,
        poll_timeout_secs: float = 0.0,
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
        key_bytes = self._serialize_key(topic=topic, key=key)
        value_bytes = self._serialize_value_payload(topic=topic, payload=value)
                
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
        value_bytes = self._serialize_value_payload(topic=topic, payload=payload)
        
        print('send')
        self.producer.produce(
            topic=topic,
            value=value_bytes,
            on_delivery=_send_delivery_report,
        )

        # Serve on_delivery callbacks from previous calls to produce()
        self.producer.poll(timeout=poll_timeout_secs)
    
    def _serialize_key(self, topic: str, key: str) -> bytes | None:
        """
        Serialize the key for the specified topic.

        Args:
            topic (str): The name of the topic.
            key (str): The key to serialize.

        Returns:
            bytes | None: The serialized key.

        Raises:
            Exception: If serialization fails.
        """
        try:
            # Use StringSerializer from Confluent's Python Client to serialize the key
            key_serializer = StringSerializer()
            # Pass the topic, key, and the message field (KEY) to the serializer
            return key_serializer(
                key, 
                SerializationContext(topic, MessageField.KEY)
            )
        except Exception as e:
            # Raise an exception with the error message
            raise Exception(f"Key serialization failed: {e}") from e


    def _serialize_value_payload(self, topic: str, payload: dict) -> bytes | None:
        """
        Serialize the event payload using the AvroSerializer.

        Args:
            topic (str): The name of the topic.
            payload (dict): The event payload to send.

        Returns:
            bytes | None: The serialized event payload.

        Raises:
            Exception: If serialization fails.
        """
        try:
            print('serialize')
            return self.value_serializer(
                payload,
                SerializationContext(topic, MessageField.VALUE),
            )
        except Exception as e:
            raise Exception(f"Serialization failed: {e}") from e

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


def has_valid_value_serializer(producer: AvroProducer) -> bool:
    logger.info("Checking if AvroSerializer is set.")
    result = False
    if producer.value_serializer is not None:
        result = True
    else:
        logger.error("AvroSerializer not set. Call set_avro_serializer() first.")
    return result



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


def _send_delivery_report(err: Optional[str], msg: Message) -> None:
    """
    A function that sends a delivery report based on the error and message received.

    Args:
        err: The error that occurred during message delivery.
        msg: The message object containing delivery information.

    Returns:
        None
    """
    print('kafka')
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
        Exception: If payload is not a dictionary.
        Exception: If the key field is not found in the payload.
    """
    if not isinstance(payload, dict):
        raise Exception("The input for 'payload' must be a dictionary.")
    
    if payload.get(key_field_name, None) is None:
        raise Exception(f"Could not find '{key_field_name}' in payload dictionary")
    
    key_value = payload.pop(key_field_name)
    return str(key_value), payload


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



def _confirm_topic_creation(result_dict: dict) -> bool:
    """Confirm topic creation was successful

    This function iterates over the dictionary of futures created by the topic 
    creation process to check if each topic creation was successful.

    Args:
        result_dict (dict):
            A dict of futures for each topic, keyed by the topic name.
            The future result() method returns None if successful.

    Returns:
        bool: True if all topics were created successfully, False otherwise.
    """
    result = True
    for topic, future in result_dict.items():
        if future.result() is not None:  
            logger.info(f"Failed to create topic '{topic}': {future.result()}")
            result = False
        else:
            # The future result() method returns None if successful.
            logger.success(f"Topic '{topic}' successfully created")
            time.sleep(1)
    return result


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
