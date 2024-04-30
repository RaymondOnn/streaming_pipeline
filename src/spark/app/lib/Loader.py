from app.lib.Logger import Log4j
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType


def get_csv_schema():
    return StructType(
        [
            StructField("_c0", IntegerType(), True),
            StructField("trans_date_trans_time", TimestampType(), True),
            StructField("cc_num", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amt", FloatType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", IntegerType(), True),
            StructField("lat", FloatType(), True),
            StructField("long", FloatType(), True),
            StructField("city_pop", IntegerType(), True),
            StructField("job", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("trans_num", StringType(), True),
            StructField("unix_time", StringType(), True),
            StructField("merch_lat", FloatType(), True),
            StructField("merch_long", FloatType(), True),
            StructField("is_fraud", BooleanType(), True),
        ]
    )


def read_events(spark, kafka_config: dict[str, str]) -> DataFrame:
    """Get streaming events from Kafka broker

    Args:
        spark_session (SparkSession): Instance of SparkSession
        kafka_config (dict[str, str]): Read configurations for Kafka

    Returns:
        DataFrame: _description_
    """
    df = spark.readStream.format("kafka").options(**kafka_config).load()
    Log4j(spark).info(f'Listening to topic "{kafka_config["subscribe"]}"...')
    return df
