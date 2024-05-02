import io
from unittest.mock import call
from unittest.mock import create_autospec
from unittest.mock import patch

import pytest
from minio import Minio

from src.flask_api.app.fx import object_store


@pytest.fixture
def mock_client():
    with patch("minio.Minio") as MockClass:
        instance = MockClass.return_value
        # instance.get_data.return_value = 'bar'
        yield instance


@pytest.fixture
def mock_minio():
    mock = create_autospec(Minio)
    yield mock


# def test_foo():
#    with patch("minio.Minio") as fake_minio:
#        client = object_store.get_client("localhost:9000", "user", "password")
#        print(fake_minio.call_count)
#        # print(client.call_count)
#        assert 1 == 2


def test_get_bucket_not_exists(mock_client):
    mock_client.bucket_exists.return_value = False
    expected = [call.bucket_exists("test"), call.make_bucket("test")]
    object_store.get_bucket(mock_client, "test")
    assert mock_client.mock_calls == expected


def test_get_bucket_exists(mock_client):
    mock_client.bucket_exists.return_value = True
    expected = [call.bucket_exists("test")]
    object_store.get_bucket(mock_client, "test")
    assert mock_client.mock_calls == expected


def test_put_json(mock_client):
    object_store.put_json(
        minio_client=mock_client,
        bucket_name="test_bucket",
        object_name="test.txt",
        d={"key": "value"},
    )
    _, kwargs = mock_client.put_object.call_args_list[0]
    assert kwargs["bucket_name"] == "test_bucket"
    assert kwargs["object_name"] == "test.txt"
    assert isinstance(kwargs["data"], io.BytesIO)
    assert kwargs["length"] == 16
    assert kwargs["content_type"] == "application/json"
