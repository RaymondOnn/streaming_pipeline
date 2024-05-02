from dataclasses import dataclass
from unittest.mock import call
from unittest.mock import create_autospec
from unittest.mock import patch

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

from src.flask_api.app.fx import pubsub
from src.flask_api.app.fx.pubsub import AvroProducer


@pytest.fixture
def mock_producer():
    mock = create_autospec(AvroProducer)
    yield mock


@pytest.fixture
def mock_client():
    mock = create_autospec(AdminClient)
    yield mock


def test_error_callback_func():
    with pytest.raises(KafkaException) as exc:
        pubsub._error_callback_func("test")
    exc_msg = exc.value.args[0]
    assert exc_msg == "test"


@dataclass
class TopicInfo:
    topic: str


def test__topic_exists(mock_client: AdminClient):
    metadata = mock_client.list_topics.return_value
    topic_list = metadata.topics.values.return_value
    topic_list.__iter__.return_value = [TopicInfo("test")]

    result = pubsub._topic_exists(client=mock_client, topic="test")
    expected = [
        call.list_topics(),  # get metadata object
        call.list_topics().topics.values(),  # extract list of topic
        call.list_topics()
        .topics.values()
        .__iter__(),  # iterate over list of topics  # noqa E501
    ]
    assert mock_client.mock_calls == expected
    assert result


def test__topic_not_exists(mock_client: AdminClient):
    metadata = mock_client.list_topics.return_value
    topic_list = metadata.topics.values.return_value
    topic_list.__iter__.return_value = [TopicInfo("test")]

    result = pubsub._topic_exists(client=mock_client, topic="not_test")
    expected = [
        call.list_topics(),  # get metadata object
        call.list_topics().topics.values(),  # extract list of topic
        call.list_topics()
        .topics.values()
        .__iter__(),  # iterate over list of topics  # noqa E501
    ]
    assert mock_client.mock_calls == expected
    assert not result


# TODO: why replicator_factor ignored by NewTopic
def test__create_topic(mock_client: AdminClient):
    pubsub._create_topic(
        client=mock_client,
        topic="testing",
        num_partitions=2,
        replication_factor=1,  # noqa E501
    )
    mock_client.create_topics.assert_called_once_with(
        new_topics=[NewTopic(topic="testing", num_partitions=2)]
    )


def test_send_message(mock_producer: pubsub.AvroProducer):
    with patch("confluent_kafka.Producer") as fake_producer:
        with patch(
            "confluent_kafka.schema_registry.avro.AvroSerializer"
        ) as fake_serializer:
            mock_producer.producer = fake_producer
            mock_producer.value_serializer = fake_serializer

            AvroProducer.send_message(
                self=mock_producer,
                topic="test",
                payload={"key": "value"},
                poll_timeout_secs=1.0,
            )

            fake_producer.poll.assert_called_once_with(timeout=1.0)
            fake_producer.flush.assert_called_once()
            # fake_producer.produce.assert_called_once_with(
            # topic='test',
            # value=mock_producer.value_serializer,
            # on_delivery=pubsub._send_delivery_report
            # )


# TODO: Figure out the mock_id issue
def test_send_message_with_key(mock_producer: pubsub.AvroProducer):
    with patch("confluent_kafka.Producer") as fake_producer:
        with patch(
            "confluent_kafka.schema_registry.avro.AvroSerializer"
        ) as fake_serializer:
            mock_producer.producer = fake_producer
            mock_producer.value_serializer = fake_serializer

            AvroProducer.send_message(
                mock_producer, "test", {"key": "value"}, 1.0
            )  # noqa E501

            fake_producer.poll.assert_called_once_with(timeout=1.0)
            fake_producer.flush.assert_called_once()
            # fake_producer.produce.assert_called_once_with(
            # topic='test',
            # value=mock_producer.value_serializer,
            # on_delivery=pubsub._send_delivery_report
            # )
