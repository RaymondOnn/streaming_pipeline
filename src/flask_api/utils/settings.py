import json
import os

import yaml
from confz import BaseConfig
from confz import EnvSource
from pydantic import AliasChoices  # noqa F401
from pydantic import AnyUrl  # noqa F401
from pydantic import Field
from pydantic import PostgresDsn  # noqa F401
from pydantic import SecretStr  # noqa F401

ENV_FILE = r"./src/flask_api/.env"
ENV_NESTED_SEP = "."


def _running_in_docker():
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")
        or os.path.isfile(path)
        and any("docker" in line for line in open(path))
    )


class APIConfig(BaseConfig):
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


class PostgresConfig(BaseConfig):
    driver: str = "postgresql"
    username: str
    password: str
    db_name: str
    host: str  # server: '127.0.0.1'
    port: str

    CONFIG_SOURCES = EnvSource(
        allow_all=True,
        prefix="PG_",
        file=ENV_FILE,
        nested_separator=ENV_NESTED_SEP,
    )

    @property
    def uri(self) -> str:
        return f"{self.driver}:// \
            {self.username}:{self.password} \
            @{self.host}:{self.port}/{self.db_name}"


class MinioConfig(BaseConfig):
    hostname: str
    port: int
    user: str = Field(min_length=3)
    password: str = Field(min_length=8)

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
        if _running_in_docker():
            return f"{self.hostname}:{str(self.port)}"
        else:
            return f"localhost:{str(self.port)}"

    @property
    def endpoint_url_lc(self) -> str:
        return f"localhost:{str(self.port)}"


class SchemaRegistryConfig(BaseConfig):
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
        if _running_in_docker():
            return f"http://{self.hostname}:{str(self.port)}"
        else:
            return f"http://localhost:{str(self.port)}"


class KafkaConfig(BaseConfig):
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
        if _running_in_docker():
            return f"{self.broker_name}:{str(10_000 + self.broker_port)}"
        else:
            return f"localhost:{str(self.broker_port)}"


class FXConfig(BaseConfig):
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
