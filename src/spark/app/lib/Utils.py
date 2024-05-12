from .Config import get_spark_conf
from .Config import get_sr_conf
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession

CONFIG_FILE = "app.conf"

def get_spark_session() -> SparkSession:
    """
    Initialize SparkSession

    Returns:
        SparkSession: required entrypoint for spark applications

    Configurations:
        - Set autoBroadcastJoinThreshold to -1 to avoid broadcasting
        - Disable adaptive execution to avoid costly re-computation
        - Set driver log4j config to log4j.properties in the project root
        - Set the master to local[2] to run on the local machine with 2 cores
        - Enable Hive support to allow for HiveQL queries
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




def get_sr_client() -> SchemaRegistryClient:
    """Get SchemaRegistryClient interface for working with Schema Registry

    Returns:
        SchemaRegistryClient: interface for working with Schema Registry

    """
    # Get Schema Registry config
    sr_conf = get_sr_conf()
    # Create SchemaRegistryClient instance
    sr_client = SchemaRegistryClient({"url": sr_conf["endpoint_url"]})
    return sr_client


def get_avro_schema(sr_client: SchemaRegistryClient, sr_subject: str) -> str:
    """
    Get the latest schema from Schema Registry for the given subject.

    Parameters
    ----------
    sr_client : SchemaRegistryClient
        Interface for working with Schema Registry
    sr_subject : str
        Unique identifier for schema

    Returns
    -------
    str
        Avro schema definition in JSON format

    """
    print("Requesting for schema...")
    # TODO: check if need to add "-value" for subject param
    schema_curr = sr_client.get_latest_version(subject_name=sr_subject)
    schema_str: str = schema_curr.schema.schema_str
    print("Schema Obtained...")
    return schema_str
