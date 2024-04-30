import uuid

from app.lib import Config
from app.lib import Loader
from app.lib import Logger
from app.lib import Transformations
from app.lib import Utils
from app.lib import Writer
from pyspark.sql import functions as F  # noqa F401
from pyspark.sql import SparkSession  # noqa F401
from pyspark.sql.types import StringType


def create_deterministic_uuid(some_string):
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"something:{some_string}"))


if __name__ == "__main__":
    # spark = SparkSession \
    #     .builder \
    #     .appName("File Streaming Demo") \
    #     .master("local[3]") \
    #     .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    #     .config("spark.sql.streaming.schemaInference", "true") \
    #     .getOrCreate()

    spark = Utils.get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.udf.register(
        create_deterministic_uuid.__name__,
        create_deterministic_uuid,
        StringType(),
    )
    logger = Logger.Log4j(spark)
    logger.info("Log4j logger initialized...")

    # csv_schema = Loader.get_csv_schema()
    # logger.info("Reading from csv source...")
    # raw_df = spark.readStream \
    #     .format("csv") \
    #     .schema(csv_schema) \
    #     .option("header", True) \
    #     .option("maxFilesPerTrigger", 1) \
    #     .load(r'/opt/spark/workdir/app/data')
    raw_df = Loader.read_events(spark, kafka_config=Config.get_kafka_conf())
    # raw_df.printSchema()

    sr = Utils.get_sr_client()
    avro_schema = Utils.get_avro_schema(sr, Config.get_sr_conf()["subject"])
    # print(avro_schema)

    print("Decoding bytes back to avro...")
    stg_df = Transformations.decode_from_avro(raw_df, avro_schema)
    # stg_df.printSchema()

    # generate ids
    stg_df = Transformations.create_id_cols(stg_df)
    stg_df = Transformations.choose_cols(stg_df)
    # stg_df.printSchema()
    Writer.write_to_console(stg_df)

    print("Writing to sink..")
    sink_df = Writer.write_to_sink(stg_df, interval_str="2 seconds")
    sink_df.awaitTermination(timeout=30)
    logger.info("App stopped due to predefined timeout")
