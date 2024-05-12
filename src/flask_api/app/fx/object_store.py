import io
import json
from typing import Any

from minio import Minio
from minio.helpers import ObjectWriteResult

from ..utils.settings import MinioConfig
from ..utils.log import logger


# https://kellner.io/minio.html

cfg = MinioConfig()



def get_minio_client(
    endpoint_url: str = cfg.endpoint_url,
    username: str = cfg.user,
    password: str = cfg.password,
    secure: bool = False,
) -> Minio:
    """Create a MinIO client object.

    Creates an instance of the MinIO client object that can be used to interact
    with a MinIO server.

    Args:
        endpoint_url: The URL of the MinIO server.
        username: The access key for the MinIO server.
        password: The secret key for the MinIO server.
        secure: Whether to use secure communication with the MinIO server.

    Returns:
        An instance of the MinIO client object.
    """
    logger.debug(f"Creating Minio client with "
        f"endpoint={endpoint_url}, "
        f"access_key={username}, "
        f"secret_key={password}, "
        f"secure={secure}")
    client = Minio(
        endpoint=endpoint_url,
        access_key=username,
        secret_key=password,
        secure=secure,
    )
    logger.debug(f"Created Minio client instance: {client}")
    logger.info(
        "Created an instance of Minio "
        "(endpoint=%s, access_key=%s, secret_key=%s, secure=%s)",
        endpoint_url,
        username,
        password,
        secure,
    )
    return client



    


def get_bucket(minio_client: Minio, bucket_name: str) -> None:
    """
    Creates a bucket in MinIO with the given name if it does not already exist.

    Args:
        minio_client (Minio): MinIO client.
        bucket_name (str): name of bucket.
    """
    logger.debug(f"Checking if bucket '{bucket_name}' exists")
    found = minio_client.bucket_exists(bucket_name)
    logger.debug(f"Bucket '{bucket_name}' {'does' if found else 'does not'} exist")
    if not found:
        logger.debug(f"Creating bucket '{bucket_name}'")
        minio_client.make_bucket(bucket_name)
        logger.success(f"Created bucket '{bucket_name}'.")
    else:
        logger.info(f"Bucket '{bucket_name}' already exists.")


def put_json(
    minio_client: "Minio",
    bucket_name: str,
    object_name: str,
    d: dict[str, Any],
) -> ObjectWriteResult:
    """Jsonify a dict and write it as object to the bucket

    This function jsonifies a dictionary, creates a BytesIO object from the
    json string, and uploads it as an object to the specified bucket using
    the MinIO client.

    Args:
        minio_client (Minio): MinIO Client
        bucket_name (str): name of bucket
        object_name (str): name of file object
        d (dict[str, Any]): data in dictionary format

    Returns:
        dict: The metadata of the uploaded object
    """
    # prepare data and corresponding data stream
    data = json.dumps(d).encode("utf-8")

    byte_stream = io.BytesIO(data)
    byte_stream.seek(0)

    logger.success(
        f"Uploaded as object {object_name}"
        + f" to bucket '{bucket_name}'"
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
