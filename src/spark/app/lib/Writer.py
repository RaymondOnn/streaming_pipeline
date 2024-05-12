import logging

from .Config import get_db_conf
from pyspark.sql import DataFrame


db_config = get_db_conf()
logging.debug(db_config.items())

# DB Configs
# -----------
# Configuration for writing to Postgres
# See: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

# Server and Port
# ----------------
# The hostname and port of the database
SERVER = db_config.get("server")
"""str: Hostname of the Postgres server"""
PORT = db_config.get("port")
"""int: Port of the Postgres server"""

# User and Password
# ------------------
# The username and password for the database
USER = db_config.get("user")
"""str: Username for the Postgres database"""
PASS = db_config.get("password")
"""str: Password for the Postgres database"""

# Database Name
# --------------
# The name of the database to connect to
DB_NAME = db_config.get("db_name")
"""str: Name of the Postgres database"""

# Schema
# ------
# The schema to use in the database
SCHEMA = db_config.get("schema")
"""str: The schema to use in the database"""

logging.debug(db_config.items())


def _write_to_postgres(df: DataFrame, epoch_id) -> None:
    # will create table before writing if not exists
    # tbl_name = df.select("table_name").limit(1).collect()[0][0]
    # df = df.drop("table_name")
    (    
        df.write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{SERVER}:{PORT}/{DB_NAME}") 
        .option("driver", "org.postgresql.Driver") 
        .option("dbtable", f"{SCHEMA}.transactions") 
        .option("user", USER) 
        .option("password", PASS) 
        .mode("append") 
        .save() 
    )
    print("Inserted data into postgres..")


def write_to_console(df: DataFrame) -> None:
    (
        df.writeStream 
        .outputMode("append") 
        .format("console") 
        .trigger(processingTime="2 seconds") 
        .start()
    )    


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
