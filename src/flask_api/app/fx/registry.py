from dataclasses import dataclass
from typing import Optional, Type

import py_avro_schema as pas
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError

from ..utils.settings import SchemaRegistryConfig
from ..utils.log import logger


SCHEMA_TYPE: str = "AVRO"
cfg = SchemaRegistryConfig()


@dataclass
class SchemaRegistry:
    """Schema Registry

    This class provides a wrapper around the Confluent Schema Registry
    client to make it easier to use.
    """

    endpoint_url: str
    

    def __post_init__(self) -> None:
        schema_config = {
            "url": self.endpoint_url,
        }
        self.sr_client = SchemaRegistryClient(schema_config)
        
        logger.debug("Created an instance of SchemaRegistry("
                + f"endpoint_url={self.endpoint_url}): {self.sr_client}",
        )

    # TODO: What if schema_str is just a string and not valid schema_str
    def register_schema(
        self,
        topic: str,
        schema_str: Optional[str] = None,
        class_object: Optional[Type] = None,
    ) -> Optional[int]:
        """
        Register a schema with the schema registry using the topic name
        and either a schema_str or class_object.

        Args:
            topic (str): Kafka topic name
            schema_str (str, optional): Avro schema in json format.
            class_object (Type, optional): Any dataclass or pydantic model.
                NOTE: Either `schema_str` or `class_object` must be provided.

        Returns:
            int: The id of the registered schema

        Raises:
            Exception: If both `schema_str` and `class_object` are provided,
                or if neither is provided.
        """
        try:
            schema_id = None
            object_name = None
            
            if schema_str is not None and class_object is not None:
                raise Exception(
                    "Cannot pass both 'schema_str' and 'class_object'"
                )

            if schema_str is None and class_object is None:
                raise Exception("Must pass either 'schema_str' or 'class_object'")

            if class_object is not None:
                schema_str = make_schema_str(class_object)
                object_name = class_object.__name__

            new_schema = Schema(schema_str, schema_type=SCHEMA_TYPE)
            schema_id = self.sr_client.register_schema(
                subject_name=get_subject_name(topic), schema=new_schema
            )
            if object_name:
                logger.success(f"Registered schema for {object_name}...")
            else:
                logger.success("Registered schema...")

        except SchemaRegistryError as e:
            logger.exception(e)
            return None
        finally:
            return schema_id




    def _delete_subject(self, topic: str) -> list:
        """Delete all versions of a schema

        Args:
            topic (str): Kafka topic name

        Returns:
            list: List of ids of deleted versions
        """
        return self.sr_client.delete_subject(get_subject_name(topic))


    # TODO: check return type
    def update_schema(self, topic: str, schema_str: str) -> Optional[int]:
        """Update the schema for a topic

        This will first delete all versions of the schema and then
        register the new schema.

        Args:
            topic (str): Kafka topic name
            schema_str (str): Avro schema in json format

        Returns:
            int: The id of the registered schema
        """
        versions_deleted_list = self._delete_subject(topic)
        logger.info("versions of schema deleted list:", versions_deleted_list)
        return self.register_schema(topic, schema_str)

    @logger.catch
    def get_schema(self, topic: str) -> str:
        """Get the latest schema for a topic

        Args:
            topic (str): Kafka topic name

        Returns:
            str: The latest schema in json format
        """
        latest_schema = self.sr_client.get_latest_version(
            get_subject_name(topic),
        )
        return latest_schema.schema.schema_str

    def make_serializer(self, schema_str: str) -> AvroSerializer:
        """Create avro_serializer that is correctly configured

        Args:
            schema_str (str): avro schema in json format

        Returns:
            AvroSerializer: _description_
        """
        try:
            return AvroSerializer(
                schema_registry_client=self.sr_client,
                schema_str=schema_str,
                conf={"auto.register.schemas": False},
            )
        except SchemaRegistryError as e: 
            logger.exception(e)
            raise Exception(f"Failed to create serializer: {e}")

    def make_deserializer(self, schema_str: str) -> AvroDeserializer:
        """Create avro_deserializer that is correctly configured

        Args:
            schema_str (str): avro schema in json format

        Returns:
            AvroDeserializer: _description_
        """
        try:
            return AvroDeserializer(
                schema_registry_client=self.sr_client, schema_str=schema_str
            )
        except SchemaRegistryError as e:
            logger.exception(e)
            raise Exception(f"Failed to create deserializer: {e}")    

def get_subject_name(topic: str) -> str:
    """
    Get the subject name for a given topic.

    Args:
        topic (str): The topic for which to get the subject name.

    Returns:
        str: The subject name derived from the topic.
    """
    return f"{topic}-value"


def make_schema_str(class_object: Type) -> str:
    """
    Generate Avro schema in JSON format from a Python class object.

    This function uses the py_avro_schema library to generate an Avro schema
    from a Python class object. The schema is returned in JSON format.

    Args:
        class_object (Type): The Python class to generate a schema for.

    Returns:
        str: The Avro schema in JSON format.
    """
    try:
        # Use the py_avro_schema library to generate an Avro schema
        # from the class object.
        schema_bytes = pas.generate(
            class_object,
            # Use JSON indent of 2 and no auto namespace
            options=pas.Option.JSON_INDENT_2 | pas.Option.NO_AUTO_NAMESPACE,
        )
        # Decode the bytes to a string
        return schema_bytes.decode()
    except Exception as e:
        logger.exception(e)
        raise Exception(f"Failed to generate schema: {e}")

def get_schema_registry_client(
        endpoint_url: str = cfg.endpoint_url,
) -> "SchemaRegistry":
    """Create an instance of SchemaRegistry

    This function creates an instance of the SchemaRegistry class
    using the endpoint_url as the URL of the Schema Registry.

    Args:
        endpoint_url (str, optional): The URL of the Schema Registry.
            Defaults to cfg.endpoint_url.

    Returns:
        SchemaRegistry: An instance of SchemaRegistry.
    """
    return SchemaRegistry(endpoint_url=endpoint_url)
