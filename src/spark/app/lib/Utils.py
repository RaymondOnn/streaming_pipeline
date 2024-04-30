from app.lib.Config import get_spark_conf
from app.lib.Config import get_sr_conf
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession

CONFIG_FILE = "app.conf"

def get_spark_session():
    """Initialize SparkSession

    Returns:
        SparkSession: required entrypoint for spark applications
    """
    print("SparkSession Created...")
    return (
        SparkSession.builder
        .config(conf=get_spark_conf(CONFIG_FILE))
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.adaptive.enabled", "false")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configuration=file:log4j.properties",
        )
        .master("local[2]")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_sr_client():
    return SchemaRegistryClient({"url": get_sr_conf()["endpoint_url"]})


def get_avro_schema(sr_client: SchemaRegistryClient, sr_subject: str):
    """Getting avro schema from Schema Registry

    Args:
        sr_client (SchemaRegistryClient): Interface for working with Schema Registry   # noqa E501
        sr_subject (str): Unique identifier for schema

    Returns:
        str: avro schema definition in JSON format
    """
    print("Requesting for schema...")
    schema_curr = sr_client.get_latest_version(sr_subject)
    schema_str = schema_curr.schema.schema_str
    print("Schema Obtained...")
    return schema_str
