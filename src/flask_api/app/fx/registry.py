from dataclasses import dataclass

import py_avro_schema as pas
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer

SCHEMA_TYPE: str = "AVRO"


def get_subject_name(topic: str) -> str:
    return f"{topic}-value"


def make_schema_str(class_object) -> str:
    return pas.generate(
        class_object,
        options=pas.Option.JSON_INDENT_2 | pas.Option.NO_AUTO_NAMESPACE,
    ).decode()


@dataclass
class SchemaRegistry:
    endpoint_url: str

    def __post_init__(self) -> None:
        schema_config = {
            "url": self.endpoint_url,
        }
        self.sr_client = SchemaRegistryClient(schema_config)

    # TODO: What if schema_str is just a string and not valid schema_str
    def register_schema(self, topic: str, schema_str: str):
        new_schema = Schema(schema_str, schema_type=SCHEMA_TYPE)
        return self.sr_client.register_schema(
            subject_name=get_subject_name(topic),
            schema=new_schema,
        )

    def _delete_subject(self, topic: str) -> list:
        return self.sr_client.delete_subject(get_subject_name(topic))

    # TODO: check return type
    def update_schema(self, topic: str, schema_str: str) -> int:
        # versions_deleted_list =
        print("versions of schema deleted list:", self._delete_subject(topic))
        return self.register_schema(topic, schema_str)

    def get_schema(self, topic: str) -> str:
        latest_schema = self.sr_client.get_latest_version(
            get_subject_name(topic),
        )
        return latest_schema.schema.schema_str

    def make_serializer(self, schema_str: str) -> AvroSerializer:
        return AvroSerializer(
            schema_registry_client=self.sr_client,
            schema_str=schema_str,
            conf={"auto.register.schemas": False},
        )

    def make_deserializer(self, schema_str: str) -> AvroDeserializer:
        return AvroDeserializer(
            schema_registry_client=self.sr_client, schema_str=schema_str
        )

    # def schema_matches(self, topic, schema_str:str) -> bool:
    # # Get the latest schema from the registry and serialize the message with it  # noqa E501
    #     if self.test_compatibility(f"{topic}-value", schema_str, schema_type=schema_type):  # noqa E501
    #         return True
    #     logging.info("Schema is not compatible with the latest version")
    #     return False
