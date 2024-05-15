from dataclasses import dataclass
from unittest.mock import call
from unittest.mock import create_autospec
from unittest.mock import patch

import py_avro_schema as pas
import pytest
from confluent_kafka.schema_registry import Schema


from src.flask_api.app.fx import registry
from src.flask_api.app.fx.registry import SchemaRegistry


@dataclass
class MockObject:
    key: str
    value: str


@pytest.fixture
def fake_registry():
    """
    Creates a fixture for a mock object of the 
    `registry.SchemaRegistryClient` class 

    Yields:
        MagicMock: A mock object for the `SchemaRegistryClient` class.
    """
    mock = create_autospec(registry.SchemaRegistryClient)
    yield mock


@pytest.fixture
def fake_sr(fake_registry):
    """
    A fixture that creates a mock object for the `SchemaRegistry` class 
    from the `registry` module.
    
    Parameters:
        fake_registry (MagicMock): A mock object for 
        the `registry.SchemaRegistryClient` class from the `registry` module.
    
    Yields:
        MagicMock: A mock object of the `SchemaRegistry` class with 
        the `sr_client` attribute attached to it.
    """
    mock = create_autospec(registry.SchemaRegistry)
    mock.attach_mock(fake_registry, "sr_client")
    yield mock


@pytest.fixture
def fake_schema_str():
    """
    Fixture that generates a fake schema string.

    Returns:
        str: The generated fake schema string.
    """
    yield pas.generate(MockObject).decode()


def test_schema_type():
    """
    Checks if registry.SCHEMA_TYPE is set to "AVRO". 
    
    Parameters:
        None

    Returns:
        None

    Raises:
        AssertionError: If the schema type of the registry is not "AVRO".
    """
    assert registry.SCHEMA_TYPE == "AVRO"


def test_get_subject_name():
    assert registry.get_subject_name("test") == "test-value"


def test_get_admin_client():
    """
    GIVEN the SchemaRegistry's endpoint_url
    WHEN registry.get_schema_registry_client function is called with the Schema Registry endpoint url
    THEN a `SchemaRegistry` instance is returned, created using the Schema Registry endpoint url
    """
    with patch("src.flask_api.app.fx.registry.SchemaRegistry") as schema_registry_cls:
        schema_registry = schema_registry_cls.return_value

        schema_registry_instance = registry.get_schema_registry_client()

        print(schema_registry_cls.mock_calls)
        schema_registry_cls.assert_called_once_with(endpoint_url='http://localhost:8081')
        assert schema_registry_instance == schema_registry



def test_register_schema(fake_sr, fake_schema_str):
    """
    GIVEN a topic and a schema string,
    WHEN SchemaRegistry.register_schema(<topic>, <schema_str>) is called
    THEN the schema is registered with the SchemaRegistryClient under the 
        subject name <topic>-value
    """
    registry.SchemaRegistry.register_schema(fake_sr, "test", fake_schema_str)
    fake_sr.sr_client.register_schema.assert_called_once_with(
        subject_name="test-value", schema=Schema(fake_schema_str, registry.SCHEMA_TYPE)
    )

    
    
def test_update_schema(fake_sr, fake_schema_str):
    """
    Test the update_schema method of the SchemaRegistry class.

    GIVEN the topic and a schema string
    WHEN SchemaRegistry.update_schema(<topic>, <schema_str>) is called
    THEN the method should, in order: 
        - Call SchemaRegistry._delete_subject(<subject_name>)
        - Call SchemaRegistry.register_schema(<subject_name>, <schema_str>)
    """
    expected = [
        call._delete_subject('test'),
        call.register_schema("test", fake_schema_str),
    ]

    registry.SchemaRegistry.update_schema(fake_sr, "test", fake_schema_str)

    print(fake_sr.mock_calls)
    assert fake_sr.mock_calls == expected


def test_get_schema(fake_sr, fake_registry):
    """
    GIVEN the Kafka topic
    WHEN SchemaRegistry.get_schema is called
    THEN the function should internally call 
        SchemaRegistryClient.get_latest_version(<subject_name>) to get the 
        latest schema
    """
    fake_sr.sr_client = fake_registry
    
    registry.SchemaRegistry.get_schema(fake_sr, "test")

    print(fake_sr.mock_calls)
    expected = [call.sr_client.get_latest_version("test-value")]
    fake_sr.assert_has_calls(expected)


def test_make_serializer(fake_sr, fake_schema_str, fake_registry):
    """
    GIVEN a schema string
    WHEN registry.SchemaRegistry.make_serializer(<schema_str>) is called
    THEN it should return an AvroSerializer instance with the correct configuration
    """
    with patch("src.flask_api.app.fx.registry.AvroSerializer") as serializer_cls:
        config = {"auto.register.schemas": False}
        fake_sr.sr_client = fake_registry
        serializer = serializer_cls.return_value


        serializer_instance = registry.SchemaRegistry.make_serializer(
            fake_sr, fake_schema_str
        )
        
        print(fake_sr.mock_calls, serializer_cls.mock_calls)
        serializer_cls.assert_called_once_with(
            schema_registry_client=fake_registry, 
            schema_str=fake_schema_str, 
            conf=config
        )
        print(serializer_instance, serializer)
        assert serializer_instance == serializer

def test_make_deserializer(fake_sr, fake_schema_str, fake_registry):
    """
    GIVEN a schema string
    WHEN registry.SchemaRegistry.make_deserializer(<schema_str>) is called
    THEN it should return an AvroDeserializer instance with the correct configuration
    """
    with patch("src.flask_api.app.fx.registry.AvroDeserializer") as deserializer_cls:
        fake_sr.sr_client = fake_registry
        deserializer = deserializer_cls.return_value


        deserializer_instance = registry.SchemaRegistry.make_deserializer(
            fake_sr, fake_schema_str
        )
        
        print(fake_sr.mock_calls, deserializer_cls.mock_calls)
        deserializer_cls.assert_called_once_with(
            schema_registry_client=fake_registry, 
            schema_str=fake_schema_str, 
        )
        print(deserializer_instance, deserializer)
        assert deserializer_instance == deserializer