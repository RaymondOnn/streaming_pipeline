import json
import os
from pathlib import Path

import yaml
from confz import BaseConfig
from confz import EnvSource
from pydantic import AliasChoices  # noqa F401
from pydantic import AnyUrl  # noqa F401
from pydantic import Field
from pydantic import PostgresDsn  # noqa F401
from pydantic import SecretStr  # noqa F401



def _running_in_docker() -> bool:
    """
    Check if the code is running inside a Docker container.

    This function checks if the code is running inside a Docker container by
    looking for the presence of the file /.dockerenv or the string "docker" in
    the file /proc/self/cgroup.

    Returns:
        bool: `True` if the code is running inside a Docker container,
        `False` otherwise.
    """
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")  # Check if /.dockerenv exists
        or os.path.isfile(path)  # Check if /proc/self/cgroup exists
        and any("docker" in line for line in open(path))  # Check if "docker" in /proc/self/cgroup
    )




def find(name, path):
    """
    Find a file with the specified name in the specified path.

    This function searches for a file with the specified name in the
    specified path and returns the path to the file if it exists.

    Args:
        name (str): The name of the file to find.
        path (str): The path to search in.

    Returns:
        (str): The path to the file if it exists, `None` otherwise.
    """
    for root, _, files in os.walk(path):
        if name in files:
            # print(root)
            # print(os.path.join(root, name))
            return os.path.join(root, name)
        
ENV_FILE = find('.env', Path(__file__).resolve().parents[2])
ENV_NESTED_SEP = "."

        
class APIConfig(BaseConfig):
    """
    Configuration for the API

    Attributes:
        port (int): The port number to listen on.
        endpoint (str): The endpoint to serve the API on.

        # OpenAPI Specs
        API_TITLE (str): The title of the API.
        API_VERSION (str): The version of the API.
        OPENAPI_VERSION (str): The version of the OpenAPI specs to use.
            Defaults to "3.0.3".
        OPENAPI_URL_PREFIX (str): The URL prefix for the OpenAPI specs.
            Defaults to "/".
        OPENAPI_SWAGGER_UI_PATH (str): The path for the Swagger UI.
            Defaults to "/swagger-ui".
        OPENAPI_SWAGGER_UI_URL (str): The URL for the Swagger UI.
            Defaults to "https://cdn.jsdelivr.net/npm/swagger-ui-dist/".
        OPENAPI_REDOC_PATH (str): The path for the ReDoc UI.
            Defaults to "/REDOC".
        OPENAPI_REDOC_UI_URL (str): The URL for the ReDoc UI.
            Defaults to "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js".

        CONFIG_SOURCES (list): The configuration sources to use.
    """
    port: int
    endpoint: str

    # OpenAPI Specs
    API_TITLE: str
    API_VERSION: str
    OPENAPI_VERSION: str = "3.0.3"
    OPENAPI_URL_PREFIX: str = "/"
    OPENAPI_SWAGGER_UI_PATH: str = "/swagger-ui"
    OPENAPI_SWAGGER_UI_URL: str = (
        "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"  # noqa E501
    )
    OPENAPI_REDOC_PATH: str = "/REDOC"
    OPENAPI_REDOC_UI_URL: str = (
        "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"
    )

    CONFIG_SOURCES = [
        EnvSource(
            allow_all=True,
            prefix="API_",
            file=ENV_FILE,
            nested_separator=ENV_NESTED_SEP,
            remap={
                "API_TITLE": "API_TITLE",
                "API_VERSION": "API_VERSION",
            },
        )
    ]


# class PostgresConfig(BaseConfig):
#     driver: str = "postgresql"
#     username: str
#     password: str
#     db_name: str
#     host: str  # server: '127.0.0.1'
#     port: str

#     CONFIG_SOURCES = EnvSource(
#         allow_all=True,
#         prefix="PG_",
#         file=ENV_FILE,
#         nested_separator=ENV_NESTED_SEP,
#     )

#     @property
#     def uri(self) -> str:
#         return f"{self.driver}:// \
#             {self.username}:{self.password} \
#             @{self.host}:{self.port}/{self.db_name}"


class MinioConfig(BaseConfig):
    """MinIO Configuration

    Configuration for MinIO.

    Attributes:
        hostname (str): The hostname of the MinIO server
        port (int): The port of the MinIO server
        user (str): The root user of the MinIO server
        password (str): The root password of the MinIO server
    """
    hostname: str
    port: int
    user: str = Field(min_length=3, description="The root user of the MinIO server")
    password: str = Field(min_length=8, description="The root password of the MinIO server")

    CONFIG_SOURCES = EnvSource(
        allow_all=True,
        prefix="MINIO_",
        file=ENV_FILE,
        nested_separator=ENV_NESTED_SEP,
        remap={
            "ROOT_USER": "user",
            "ROOT_PASSWORD": "password",
        },
    )

    @property
    def endpoint_url(self) -> str:
        """The endpoint URL of the MinIO server

        If we are running in Docker, return the hostname and port.
        Otherwise, return the localhost and port.

        Returns:
            str: The endpoint URL of the MinIO server
        """
        if _running_in_docker():
            return f"{self.hostname}:{str(self.port)}"
        else:
            return f"localhost:{str(self.port)}"


class SchemaRegistryConfig(BaseConfig):
    """Schema Registry Configuration

    Configuration for the Confluent Schema Registry.

    Attributes:
        hostname (str): The hostname of the Schema Registry server
        port (int): The port of the Schema Registry server
    """
    hostname: str
    port: int

    CONFIG_SOURCES = [
        EnvSource(
            allow_all=True,
            prefix="SR_",
            file=ENV_FILE,
            nested_separator=ENV_NESTED_SEP,
        )
    ]

    @property
    def endpoint_url(self) -> str:
        """The endpoint URL of the Schema Registry server

        If we are running in Docker, return the hostname and port.
        Otherwise, return the localhost and port.

        Returns:
            str: The endpoint URL of the Schema Registry server
        """
        if _running_in_docker():
            return f"http://{self.hostname}:{str(self.port)}"
        else:
            return f"http://localhost:{str(self.port)}"


class KafkaConfig(BaseConfig):
    """Kafka Configuration

    Configuration for Kafka.

    Attributes:
        broker_name (str): The name of the Kafka broker
        broker_port (int): The port of the Kafka broker
        topic (str): The Kafka topic to use
    """
    broker_name: str
    broker_port: int
    topic: str

    CONFIG_SOURCES = EnvSource(
        allow_all=True,
        prefix="KAFKA_",
        file=ENV_FILE,
        nested_separator=ENV_NESTED_SEP,
    )

    @property
    def bootstrap_servers(self) -> str:
        """The bootstrap servers for Kafka

        If we are running in Docker, return the broker name and port.
        Otherwise, return the localhost and port.

        Returns:
            str: The bootstrap servers for Kafka
        """
        if _running_in_docker():
            return f"{self.broker_name}:{str(10_000 + self.broker_port)}"
        else:
            return f"localhost:{str(self.broker_port)}"


class FXConfig(BaseConfig):
    """FX Configuration

    Configuration for the fraud detection application.

    Attributes:
        bucket_fraud (str): The name of the bucket for fraudulent files
        bucket_valid (str): The name of the bucket for valid files
    """
    bucket_fraud: str
    bucket_valid: str

    CONFIG_SOURCES = EnvSource(
        allow_all=True,
        prefix="FX_",
        file=ENV_FILE,
        nested_separator=ENV_NESTED_SEP,
    )

    @property
    def buckets(self) -> list[str]:
        """The list of buckets

        Returns:
            list[str]: The list of buckets
        """
        return list(vars(self).values())


class YamlConfig:
    def __init__(self, d):
        self.__dict__.update(d)

    def encode(self):
        return vars(self)  # returns __dict__

    @classmethod
    def from_json(cls, json_str: str):
        return cls.from_dict(json.dumps(json_str))

    @classmethod
    def from_dict(cls, dict_):
        return json.loads(dict_, object_hook=cls)

    @classmethod
    def from_yaml(cls, yaml_file):
        with open(yaml_file, encoding="utf-8") as file:
            d = yaml.safe_load(file)
            return cls.from_dict(d)

    def to_json(self, indent: int | None = None):
        return json.dumps(self, default=lambda o: o.encode(), indent=indent)

    def to_dict(self):
        return json.loads(self.to_json())


def yaml2obj(yaml_file):
    # using json.loads method and passing json.dumps
    # method and custom object hook as arguments
    try:
        with open(yaml_file, encoding="utf-8") as file:
            specs = yaml.safe_load(file)
            return json.loads(json.dumps(specs), object_hook=YamlConfig)
    except (yaml.YAMLError, FileNotFoundError) as e:
        print(e)
        raise Exception


# LogConfig = YamlConfig.from_yaml('config/log_config.yaml')

# SparkConfig = yaml2obj('spark_config.yaml')

# if __name__ == '__main__':
#     LogConfig = yaml2obj("config/log_config.yaml")
#     print(FXConfig().buckets)
