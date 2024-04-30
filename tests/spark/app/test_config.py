from src.spark.app.lib import Config
import pytest
import configparser
from unittest.mock import patch

def test_find():
    assert Config.find("test.conf", "tests") == 'tests/spark/test.conf'
    

def test_get_app_conf():
    assert Config.get_app_conf("test.conf", "SECTION") == {'test-field': 'test-value'}


def test_get_app_conf_not_found():
    with pytest.raises(Exception):
        Config.get_app_conf("not_exist.conf", "SECTION")


def test_get_app_conf_wrong_section():
    with pytest.raises(configparser.NoSectionError):
        Config.get_app_conf("test.conf", "SECTIO")


def test_get_kafka_conf():
    with patch("src.spark.app.lib.Config.get_app_conf") as fake_get_conf:
        Config.get_kafka_conf()
        fake_get_conf.assert_called_once_with(file_name='app.conf', section_name='KAFKA')


def test_get_db_conf():
    with patch("src.spark.app.lib.Config.get_app_conf") as fake_get_conf:
        Config.get_db_conf()
        fake_get_conf.assert_called_once_with(file_name='app.conf', section_name='POSTGRES')


def test_get_sr_conf():
    with patch("src.spark.app.lib.Config.get_app_conf") as fake_get_conf:
        Config.get_sr_conf()
        fake_get_conf.assert_called_once_with(file_name='app.conf', section_name='SCHEMA_REG')


def test_get_spark_conf():
    conf = Config.get_spark_conf("test.conf", "SPARK")
    assert conf.toDebugString() == "spark-field=spark-value"


def test_get_spark_conf_not_found():
    with pytest.raises(Exception):
        Config.get_spark_conf("not_exist.conf", "SPARK")
        
def test_get_spark_conf_wrong_section():
    with pytest.raises(configparser.NoSectionError):
        Config.get_app_conf("test.conf", "SPARKK")