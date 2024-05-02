from dataclasses import dataclass
from unittest.mock import call
from unittest.mock import create_autospec

import py_avro_schema as pas
import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient

from src.flask_api.app.fx import registry
from src.flask_api.app.fx.registry import SchemaRegistry


@dataclass
class MockObject:
    key: str
    value: str


@pytest.fixture
def fake_registry():
    mock = create_autospec(SchemaRegistryClient)
    yield mock


@pytest.fixture
def fake_client(fake_registry):
    mock = create_autospec(SchemaRegistry)
    mock.attach_mock(fake_registry, "sr_client")
    yield mock


@pytest.fixture
def mock_schema_str():
    yield pas.generate(MockObject).decode()


def test_schema_type():
    assert registry.SCHEMA_TYPE == "AVRO"


def test_get_subject_name():
    assert registry.get_subject_name("test") == "test-value"


def test_register_schema(fake_client, mock_schema_str):
    SchemaRegistry.register_schema(fake_client, "test", mock_schema_str)
    print(fake_client.mock_calls)
    fake_client.sr_client.register_schema.assert_called_once_with(
        subject_name="test-value",
        schema=Schema(mock_schema_str, schema_type=registry.SCHEMA_TYPE),
    )


def test_update_schema(fake_client, fake_registry, mock_schema_str):
    SchemaRegistry.update_schema(fake_client, "test", mock_schema_str)

    expected = [
        "call._delete_subject('test')",
        "call._delete_subject().__str__()",
        call.register_schema("test", mock_schema_str).__str__(),
    ]
    print(fake_client.mock_calls)
    # converting to str to lock in the string representation
    # because call._delete_subject().__str__() becomes "call._delete_subject()"
    assert [str(c) for c in fake_client.mock_calls] == expected


def test_get_schema(fake_client):
    SchemaRegistry.get_schema(fake_client, "test")

    print(fake_client.mock_calls)
    expected = [call.sr_client.get_latest_version("test-value")]
    fake_client.assert_has_calls(expected)


# def test_make_serializer(fake_client, mock_schema_str):
#         with patch('confluent_kafka.schema_registry.avro.AvroSerializer') as fake_serializer:  # noqa E501
#             SchemaRegistry.make_serializer(fake_client, mock_schema_str)
#             print(fake_client.mock_calls)
#             assert 1 == 2
