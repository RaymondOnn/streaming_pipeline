import json
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError

from ..fx import pubsub
from ..fx import object_store
from ..fx import registry
from ..utils.settings import FXConfig
from ..utils.settings import KafkaConfig


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

# Set up other services
minio_client = object_store.get_minio_client()
producer = pubsub.get_avro_producer()
schema_str = registry.make_schema_str(TransactionStream)

def generate_object_name(request_json: str) -> str:
    """Create file name based on PUT request

    Args:
        request_json (str): PUT request in json format

    Returns:
        str: name of file object to be stored in MinIO
    """
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


    obj_name = generate_object_name(request_json)
    try:
        item = TransactionStream.model_validate_json(request_json)

        if item.is_fraud:
            object_store.put_json(
                minio_client,
                FXConfig().bucket_fraud,
                obj_name,
                item.model_dump(),
            )
        else:
            producer.send_message(
                KafkaConfig().topic,
                schema_str=schema_str,
                payload=item.model_dump(),
                poll_timeout_secs=0.1,
            )
    except ValidationError as exc:
        request_json = json.dumps(
            {**json.loads(request_json), **{"errors": exc.errors()}}
        )
        object_store.put_json(
            minio_client,
            FXConfig().bucket_valid,
            obj_name,
            json.loads(request_json),
        )
