import json
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError

from .object_store import get_client
from .object_store import put_json
from .pubsub import AvroProducer
from .registry import make_schema_str
from .registry import SchemaRegistry
from flask_api.utils.settings import FXConfig
from flask_api.utils.settings import KafkaConfig
from flask_api.utils.settings import MinioConfig
from flask_api.utils.settings import SchemaRegistryConfig


class TransactionStream(BaseModel):
    trans_date_trans_time: datetime
    cc_num: str
    merchant: str
    category: str
    amt: float
    first: str
    last: str
    gender: str
    street: str
    city: str
    state: str
    zip: int
    lat: float
    long: float
    city_pop: int
    job: str
    dob: str
    trans_num: str
    unix_time: str
    merch_lat: float
    merch_long: float
    is_fraud: bool


def generate_object_name(request_json: str):
    dir_name = f"{datetime.now().strftime('%Y%m%d/%H00/')}"
    trans_num = json.loads(request_json).get("trans_num")
    return f"/{dir_name}/" + f"{trans_num}.txt"


def sort_requests(request_json: str):
    """Sort PUT requests into 3 groups
        - Failed validation: goes to "validation-errors" bucket
        - Is Fraud: goes to "fraud-transactions" bucket
        - Otherwise, continues on to Kafka broker

    Args:
        request_json (str): PUT request payload received
    """

    # Schema Registry needed to coordinate schema
    sr = SchemaRegistry(endpoint_url=SchemaRegistryConfig().endpoint_url)
    schema_str = make_schema_str(TransactionStream)

    minio_client = get_client(
        MinioConfig().endpoint_url, MinioConfig().user, MinioConfig().password
    )
    # Set up Producer for streaming
    producer = AvroProducer(
        kafka_brokers=KafkaConfig().bootstrap_servers,
        value_serializer=sr.make_serializer(schema_str),
    )

    obj_name = generate_object_name(request_json)
    try:
        item = TransactionStream.model_validate_json(request_json)

        if item.is_fraud:
            put_json(
                minio_client,
                FXConfig().bucket_fraud,
                obj_name,
                item.model_dump(),
            )
        else:
            producer.send_message(
                KafkaConfig().topic,
                payload=item.model_dump(),
                poll_timeout_secs=0.1,
            )
    except ValidationError as exc:
        request_json = json.dumps(
            {**json.loads(request_json), **{"errors": exc.errors()}}
        )
        put_json(
            minio_client,
            FXConfig().bucket_valid,
            obj_name,
            json.loads(request_json),
        )
