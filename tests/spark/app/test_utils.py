import pytest
from src.spark.app.lib import Utils

@pytest.fixture(scope='session')
def spark():
    return Utils.get_spark_session()

def test_spark_version(spark):
    print(spark.version)
    assert spark.version == "3.5.0"
    assert 1 == 2