from confluent_kafka.admin import AdminClient
from flask import Flask
from flask_smorest import Api

from flask_api.api.transactions import blp as TransactionBlueprint
from flask_api.fx import object_store
from flask_api.fx.pubsub import create_topic
from flask_api.fx.registry import make_schema_str
from flask_api.fx.registry import SchemaRegistry
from flask_api.fx.sort_requests import TransactionStream
from flask_api.utils.settings import APIConfig
from flask_api.utils.settings import FXConfig
from flask_api.utils.settings import KafkaConfig
from flask_api.utils.settings import MinioConfig
from flask_api.utils.settings import SchemaRegistryConfig


minio_client = object_store.get_client(
    MinioConfig().endpoint_url, MinioConfig().user, MinioConfig().password
)
sr = SchemaRegistry(endpoint_url=SchemaRegistryConfig().endpoint_url)
admin_client = AdminClient(
    {"bootstrap.servers": KafkaConfig().bootstrap_servers},
)


def setup():
    # Set Up Storage buckets
    for bucket in FXConfig().buckets:
        object_store.get_bucket(minio_client, bucket)
    schema_str = make_schema_str(
        TransactionStream
    )  # Schema Registry needed to coordinate schema
    sr.register_schema(
        KafkaConfig().topic, schema_str
    )  # TODO: get schema. register if not exist

    # Set up Producer for streaming
    create_topic(admin_client, KafkaConfig().topic)


def create_app():
    app = Flask(__name__)
    app.config.from_object(APIConfig())

    api = Api(app)
    api.register_blueprint(TransactionBlueprint)

    with app.app_context():  # i.e. before_first_request
        setup()
        return app


if __name__ == "__main__":
    create_app().run(port=5000)
