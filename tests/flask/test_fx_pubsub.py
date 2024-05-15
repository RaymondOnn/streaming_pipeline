from dataclasses import dataclass
from unittest.mock import call
from unittest.mock import create_autospec
from unittest.mock import patch
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import NewTopic

from src.flask_api.app.fx import pubsub



@pytest.fixture
def fake_avro_producer():
    """Create a mock Avro producer."""
    return create_autospec(pubsub.AvroProducer)


@pytest.fixture
def fake_producer():
    """Create a mock producer."""
    return create_autospec(pubsub.Producer)

@pytest.fixture
def fake_admin_client() -> Mock:
    """Create a mock admin client."""
    return create_autospec(pubsub.AdminClient)


@pytest.fixture
def fake_logger() -> Mock:
    """Create a mock logger."""
    return create_autospec(pubsub.logger)


def test_error_callback_func():
    """
    Given a Kafka error message
    When the error callback function is called with the error message
    Then it raises a KafkaException with the provided error message
    """
    with pytest.raises(KafkaException) as exc_info:
        pubsub._error_callback_func(kafka_error="test")
    assert exc_info.value.args[0] == "test"


@dataclass
class TopicInfo:
    topic: str

    
def test_topic_exists(fake_admin_client: Mock):
    """
    Test the existence of a topic in the provided admin client.

    GIVEN the admin client and a topic name and the topic exists 
    WHEN pubsub._topic_exists(<admin_client>, <topic>) is called
    THEN the function should return True
    """
    topics = [TopicInfo("test"),]
    fake_admin_client.list_topics.return_value.topics.values.return_value = topics

    assert pubsub._topic_exists(fake_admin_client, "test")
    
    """
    GIVEN the admin client and a topic name and the topic does not exists 
    WHEN pubsub._topic_exists(<admin_client>, <topic>) is called
    THEN the function should return False
    """
    assert not pubsub._topic_exists(fake_admin_client, "unknown")


# TODO: why replicator_factor ignored by NewTopic
def test__create_topic(fake_admin_client: Mock):
    """
    GIVEN an admin client, the topic name, number of partitions, 
        and replication factor
    WHEN pubsub._create_topic(<admin_client>, <topic>, <num_partitions>, 
    <replication_factor>) is called 
    THEN the function should create a new topic with the specified number of 
        partitions and replication factor
    """
    num_partitions = 2
    replication_factor = 1
    topic = "testing"
    pubsub._create_topic(
        fake_admin_client, topic, num_partitions, replication_factor
    )
    fake_admin_client.create_topics.assert_called_once_with(
        new_topics=[NewTopic(topic, num_partitions, replication_factor)]
    )



# def test__send_delivery_report(fake_logger):
    # message = Mock(spec=pubsub.Message)
    # message.topic.return_value = "test"
    # message.partition.return_value = 1
    # message.offset.return_value = 2
    # message.latency.return_value = 0.01
    # message.timestamp.return_value = (None, 6789578780976)

#     pubsub._send_delivery_report(err=None, msg=message)
#     print(fake_logger.mock_calls)
#     fake_logger.info.assert_called_once_with(
#         "Message sent: {'topic': 'test', 'topic_partition': 1, "
#         "'kafka_offset': 2, 'latency': 0.01, "
#         "'produce_kafka_time': '2185-02-25 10:06:20.976000'}"
#     )

#     with pytest.raises(KafkaException, match="test"):
#         pubsub._send_delivery_report(err="test")


def test__send_delivery_report():
    """
    GIVEN a KafkaException
    WHEN pubsub._send_delivery_report function is called with an error,
    THEN it should raise a KafkaException with the correct error message.

    GIVEN the Message object,
    WHEN pubsub._send_delivery_report function is called with a message,
    THEN it should call the necessary methods on the message object to 
        extract its attribute values
    """
    with pytest.raises(KafkaException, match="test"):
        pubsub._send_delivery_report(err="test", msg=None)

    message = Mock(spec=pubsub.Message)
    message.topic.return_value = "test"
    message.partition.return_value = 1
    message.offset.return_value = 2
    message.latency.return_value = 0.01
    message.timestamp.return_value = (None, 6789578780976)
    expected_calls = [
        call.topic(),
        call.partition(),
        call.offset(),
        call.latency(),
        call.timestamp()
    ]
    
    pubsub._send_delivery_report(err=None, msg=message)
    
    assert message.mock_calls == expected_calls


def test_extract_key():
    """
    GIVEN a dictionary and the name of the key field
    WHEN pubsub.extract_key(<payload_dict>, <key_field_name>) is called
    THEN the function should return a tuple containing the value 
        of the key_field and a dictionary containing the remaining keys 
        and values from the payload

    GIVEN the same payload dictionary
    WHEN called with the payload and a non-existent key as arguments
    THEN an exception should be raised with the message 
        "Could not find 'xxx' in payload"

    GIVEN the same payload dictionary
    WHEN called with a non-dictionary object as payload
    THEN an exception should be raised with the message
        "The input for 'payload' must be a dictionary."
    """


    payload = {'id': '1', 'name': "Bob", 'email': "bob@foo.com"}
    expected_key = '1'
    expected_rest = {'name': "Bob", 'email': "bob@foo.com"}

    assert pubsub.extract_key(payload, 'id') == (expected_key, expected_rest)

    with pytest.raises(
            Exception,
            match="Could not find 'xxx' in payload"
    ):
        pubsub.extract_key(payload, 'xxx')

    with pytest.raises(
            Exception,
            match="The input for 'payload' must be a dictionary."
    ):
        pubsub.extract_key(object(), 'id') # type: ignore


def test_send_message(fake_avro_producer, fake_producer):
    """
    GIVEN a Kafka topic and a payload dictionary
    WHEN pubsub.AvroProducer.send_message() is called
    THEN the method should, in order: 
        - send a serialized message to the specified Kafka topic via the 
        Producer.produce() internally
        - call Producer.poll() with the specified timeout
    """
    # with patch("src.flask_api.app.fx.pubsub.AvroSerializer") as fake_serializer:
    fake_avro_producer.producer = fake_producer
    fake_avro_producer._serialize_value_payload.return_value = b'some_value_bytes'

    pubsub.AvroProducer.send_message(
        fake_avro_producer,
        topic="test", 
        payload={"key1": "value1", "key2": "value2"}, 
    )

    print(fake_producer.mock_calls)
    fake_producer.poll.assert_called_once_with(timeout=0)
    fake_producer.produce.assert_called_once_with(
        topic='test',
        value=b'some_value_bytes',
        on_delivery=pubsub._send_delivery_report
    )


def test_send_message_with_key(fake_avro_producer, fake_producer):
    """
    GIVEN a Kafka topic, a payload dictionary and the name of the key field
    WHEN pubsub.AvroProducer.send_message_with_key() is called with the specified parameters
    THEN the method should, in order: 
        - send a serialized message with the specified key value as key to the 
        specified Kafka topic via the Producer.produce() internally
        - call Producer.poll() with the specified timeout
    """
    fake_avro_producer.producer = fake_producer
    fake_avro_producer._serialize_key.return_value = b'value1'
    fake_avro_producer._serialize_value_payload.return_value = b'some_value_bytes'

    pubsub.AvroProducer.send_message_with_key(
        fake_avro_producer,
        topic="test", 
        payload={"key1": "value1", "key2": "value2"}, 
        key='key1', 
    )

    print(fake_producer.mock_calls)
    fake_producer.poll.assert_called_once_with(timeout=0)
    fake_producer.produce.assert_called_once_with(
        topic='test',
        value=b'some_value_bytes',
        key=b'value1', 
        on_delivery=pubsub._send_delivery_report
    )


def test_get_avro_producer():
    """
    GIVEN the host and port of the Kafka brokers
    WHEN pubsub.get_avro_producer(<kafka_brokers>) is called with the specified Kafka brokers
    THEN an AvroProducer instance is returned, created using the specified Kafka brokers
    """
    with patch('src.flask_api.app.fx.pubsub.AvroProducer') as producer_cls:
        producer = producer_cls.return_value

        producer_instance = pubsub.get_avro_producer(kafka_brokers='test_host:9999')

        producer_cls.assert_called_once_with(kafka_brokers='test_host:9999')
        assert producer_instance == producer
        
        
def test_get_admin_client():
    """
    GIVEN the host and port of the Kafka brokers
    WHEN pubsub.get_admin_client(<kafka_brokers) is called with the specified Kafka brokers
    THEN an `AdminClient` instance is returned, created using the specified Kafka brokers
    """
    with patch("src.flask_api.app.fx.pubsub.AdminClient") as admin_client_cls:
        admin_client = admin_client_cls.return_value

        admin_client_instance = pubsub.get_admin_client(
            kafka_brokers="test_host:9999"
        )

        print(admin_client_cls.mock_calls, admin_client_cls.call_args.args)
        admin_client_cls.assert_called_once_with({"bootstrap.servers": "test_host:9999"})
        assert admin_client_instance == admin_client


def test__confirm_topic_creation_success():
    """
    GIVEN result_dict created with "test" as keyand a future object
        as value for which future.result() returns None.
    WHEN the function is called, passing in the result_dict 
    THEN logger.success() is called once with the message 
        "Topic 'test' successfully created".
    """
    with patch("src.flask_api.app.fx.pubsub.logger") as fake_logger:
        future = Mock()
        future.result.return_value = None
        result_dict = {
            "test": future
        }
        pubsub._confirm_topic_creation(result_dict)
        fake_logger.success.assert_called_once_with("Topic 'test' successfully created")


def test__confirm_topic_creation_failed(fake_logger):
    """
    GIVEN result_dict created with "test" as key and a future object
        as value for which future.result() returns a non-None value: True.
    WHEN pubsub._confirm_topic_creation(<result_dict>) called
    THEN logger.info() is called once with the message 
        "Failed to create topic 'test': {True}".
    """
    with patch("src.flask_api.app.fx.pubsub.logger") as fake_logger:
        future = Mock()
        future.result.return_value = True
        result_dict = {
            "test": future
        }
        pubsub._confirm_topic_creation(result_dict)
        print(fake_logger.mock_calls)
        fake_logger.info.assert_called_once_with(f"Failed to create topic 'test': {future.result.return_value}")

