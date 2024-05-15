import io
from unittest.mock import call
from unittest.mock import patch

import pytest
from src.flask_api.app.fx import object_store


@pytest.fixture
def fake_minio():
    """
    A fixture that creates a mock object of the `Minio` class from 
    the `src.flask_api.app.fx.object_store` module.
    
    Returns:
        MagicMock: A mock object of the `Minio` class.
    """
    with patch("src.flask_api.app.fx.object_store.Minio") as minio_mock:
        yield minio_mock

def test_get_minio_client(fake_minio):
    """
    GIVEN no issues getting the Minio configuration settings
    WHEN the `get_minio_client` function is called without any arguments.
    THEN it should create and return a `Minio` client object with 
        the supplied endpoint, access key, secret key 
        and secure flag set to False.
    """
    expected_endpoint = 'localhost:9000'
    expected_access_key = 'user'
    expected_secret_key = 'password'
    expected_secure = False

    obj_store_instance = object_store.get_minio_client()
    fake_minio.assert_called_once_with(
        endpoint=expected_endpoint,
        access_key=expected_access_key,
        secret_key=expected_secret_key,
        secure=expected_secure
    )
    assert obj_store_instance == fake_minio.return_value

    object_store.get_minio_client(
        endpoint_url="default_endpoint",
        username="default_user",
        password="default_password",
        secure=True
    )
    fake_minio.assert_called_with(
        endpoint='default_endpoint',
        access_key='default_user',
        secret_key='default_password',
        secure=True
    )


def test_get_bucket_if_not_exists(fake_minio):
    """
    GIVEN the name of a bucket that does not exists.
    WHEN function is called with that bucket name
    THEN it should check if the bucket exists and create it if it does not.
    """
    fake_minio.bucket_exists.return_value = False
    expected = [call.bucket_exists("test"), call.make_bucket("test")]
    object_store.get_bucket(fake_minio, "test")
    assert fake_minio.mock_calls == expected


def test_get_bucket_if_exists(fake_minio):
    """
    GIVEN the name of a bucket that already exists.
    WHEN function is called with that bucket name
    THEN it should check if the bucket exists but not create it.
    """
    fake_minio.bucket_exists.return_value = True

    object_store.get_bucket(fake_minio, "test")

    fake_minio.bucket_exists.assert_called_once_with("test")
    fake_minio.make_bucket.assert_not_called()


def test_put_json(fake_minio):
    """
    GIVEN a `fake_minio` mock object representing the `Minio` class.
    WHEN function is called with a bucket name, object name, and data.
    THEN in paticular order, it should:
        - create a BytesIO object from the data dict, 
        - upload it to the specified bucket and object name,
        - Note: content type to be set to "application/json".
    """
    bucket_name = "test_bucket"
    object_name = "test.txt"
    data = {"key": "value"}

    object_store.put_json(fake_minio, bucket_name, object_name, data)

    _, kwargs = fake_minio.put_object.call_args_list[0]

    # Check that the bucket name and object name are correct
    assert kwargs["bucket_name"] == bucket_name
    assert kwargs["object_name"] == object_name

    # Check that the data is a BytesIO object
    assert isinstance(kwargs["data"], io.BytesIO)

    # Check that the length is correct
    assert kwargs["length"] == 16

    # Check that the content type is correct
    assert kwargs["content_type"] == "application/json"

