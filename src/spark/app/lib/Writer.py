import logging

from app.lib import Config
from pyspark.sql import DataFrame

# db_config = {
#         'user': 'postgres',
#         'password': 'postgres',
#         'db_name': 'postgres',
#         'server': 'postgresdb',
#         'port': '5432',
#         'schema': 'staging'
#     }
db_config = Config.get_db_conf()
logging.debug(db_config.items())

SERVER = db_config.get("server")
PORT = db_config.get("port")
USER = db_config.get("user")
PASS = db_config.get("password")
DB_NAME = db_config.get("db_name")
SCHEMA = db_config.get("schema")


def _write_to_postgres(df: DataFrame, epoch_id) -> None:
    # will create table before writing if not exists
    # tbl_name = df.select("table_name").limit(1).collect()[0][0]
    # df = df.drop("table_name")
    df.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{SERVER}:{PORT}/{DB_NAME}"
    ).option("driver", "org.postgresql.Driver").option(
        "dbtable", f"{SCHEMA}.transactions"
    ).option(
        "user", USER
    ).option(
        "password", PASS
    ).mode(
        "append"
    ).save()


def write_to_console(df: DataFrame) -> None:
    df.writeStream.outputMode("append").format("console").trigger(
        processingTime="2 seconds"
    ).start()


def write_to_sink(df: DataFrame, interval_str: str = "2 seconds"):
    """Streams the contents of the DataFrame into postgres

    Args:
        df (DataFrame): Output dataframe
        interval_str (str): String indicating the specified time interval

    Returns:
        None
    """
    print("Writing to Sink")
    # .option("checkpointLocation", "chk-point-dir")
    # .withColumn("table_name", F.lit(table_name)) \
    return (
        df.writeStream.foreachBatch(_write_to_postgres)
        .outputMode("update")
        .trigger(processingTime=interval_str)
        .start()
    )
