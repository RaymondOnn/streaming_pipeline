

import json
from datetime import datetime, timezone

from pydantic import BaseModel, ValidationError
from flask import request
from flask.views import MethodView
from flask_smorest import Blueprint

from ..fx import pubsub, object_store, registry
from ..utils.settings import FXConfig, KafkaConfig


blp = Blueprint("transactions", __name__, description="abc")


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
schema_str = registry.make_schema_str(TransactionStream)
producer = pubsub.get_avro_producer()
producer.set_avro_serializer(schema_str)


def generate_object_name(request_json: str) -> str:
    """
    Generates a file name based on the contents of the PUT request
    
    The file name is in the format of 
    `/<year><month><day>/<hour>00/<transaction_number>.txt` where 
    `<year><month><day>` is the current date in UTC time,
    `<hour>` is the current hour in UTC time, and
    `<transaction_number>` is the transaction number from the request.

    Args:
        request_json (str): PUT request in json format

    Returns:
        str: name of file object to be stored in MinIO
    """
    # get current UTC time in format of %Y%m%d
    dir_name = datetime.now(timezone.utc).strftime('%Y%m%d/')
    # get current UTC hour in format of %H00
    dir_name += datetime.now(timezone.utc).strftime('%H00/')
    # get transaction number from the request
    trans_num = json.loads(request_json).get("trans_num")
    # concatenate all the parts to form the file name
    return f"/{dir_name}/" + f"{trans_num}.txt"

# TODO: Test if wrong data triggers validation error
# TODO: Test if data reaches API via POST request
# TODO: Test if data validation is as expected.
@blp.route("/transactions")
class Transaction(MethodView):
    """Endpoint for processing transactions"""

    # Validate and send the data to Kafka or a valid bucket
    def post(self):
        """
        Process a transaction by validating the data and sending it to the 
        appropriate destination.

        Returns:
            str: The original transaction data JSON response.

        Raises:
            ValidationError: If the transaction data fails validation.
        """
        resp = request.get_json()
        
        # Generates a unique object name based on the transaction data. 
        obj_name = generate_object_name(resp)
        try:
            # Validate the transaction data using the `TransactionStream` model. 
            item = TransactionStream.model_validate_json(resp)
            
            # Checks if the transaction is flagged as fraud. 
            # If flagged as fraud, transaction data is stored in the `bucket_fraud` MinIO bucket. 
            if item.is_fraud:
                object_store.put_json(
                    minio_client, FXConfig().bucket_fraud, obj_name, item.model_dump()
                )
            else:
                # Transaction data is sent to the Kafka topic specified in the configuration.
                producer.send_message(
                    KafkaConfig().topic,
                    schema_str=schema_str,
                    payload=item.model_dump(),
                    poll_timeout_secs=0.1,
                )
        except ValidationError as exc:
            
            # If the validation fails, a `ValidationError` exception is raised. 
            # JSON response modified to include the validation errors. 
            request_json = json.dumps({**json.loads(resp), "errors": exc.errors()})
            
            # The modified response is then stored in the `bucket_valid` MinIO bucket.
            object_store.put_json(
                minio_client, FXConfig().bucket_valid, obj_name, json.loads(request_json)
            )
        finally:
            return resp
