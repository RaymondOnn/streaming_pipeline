import io
import json
from typing import Any

from minio import Minio

# https://kellner.io/minio.html


def get_client(endpoint_url: str, username: str, password: str, secure=False):
    client = Minio(
        endpoint=endpoint_url,
        access_key=username,
        secret_key=password,
        secure=secure,
    )
    return client


def get_bucket(minio_client: Minio, bucket_name: str):
    # Make the bucket if it doesn't exist.
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")


def put_json(
    minio_client: Minio, bucket_name: str, object_name: str, d: dict[str, Any]
):
    """
    jsonify a dict and write it as object to the bucket
    """

    # prepare data and corresponding data stream
    data = json.dumps(d).encode("utf-8")
    byte_stream = io.BytesIO(data)
    byte_stream.seek(0)

    print(
        f"Successfully uploaded as object {object_name}  \
            to bucket {bucket_name}"
    )
    # put data as object into the bucket
    return minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=byte_stream,
        length=len(data),
        content_type="application/json",
    )


def send_to_storage(minio_client: Minio, bucket_name, object_name, data):
    get_bucket(minio_client, bucket_name)
    put_json(minio_client, bucket_name, object_name, data)
