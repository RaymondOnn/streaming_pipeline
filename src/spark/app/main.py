try:
    import sys
    import uuid
    from app.lib import Utils, Loader, Transformations, Writer, Config, Logger
    from pyspark.sql.types import StringType
except ImportError as e:
    print(f"Error: {e}")


def create_deterministic_uuid(some_string):
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"something:{some_string}"))


def main(debug: bool = False):
    # Setting things up
    spark = Utils.get_spark_session()
    # spark.sparkContext.setLogLevel("ERROR")
    spark.udf.register(
        create_deterministic_uuid.__name__,
        create_deterministic_uuid,
        StringType(),  # noqa E501
    )
    logger = Logger.Log4j(spark)
    logger.info("Log4j logger initialized...")

    # Set up Kafka Source
    raw_df = Loader.read_events(spark, kafka_config=Config.get_kafka_conf())
    if debug:
        raw_df.printSchema()

    # Get avro schema which is required for decoding back to avro
    sr = Utils.get_sr_client()
    avro_schema = Utils.get_avro_schema(sr, Config.get_sr_conf()["subject"])
    if debug:
        print(avro_schema)

    # Decoding back from avro bytes
    logger.info("Decoding bytes back to avro...")
    stg_df = Transformations.decode_from_avro(raw_df, avro_schema)
    if debug:
        stg_df.printSchema()

    # Generating identifier columns first to avoid joining later
    stg_df = Transformations.create_id_cols(stg_df)
    stg_df = Transformations.choose_cols(stg_df)
    logger.info("Transformations completed and ready for sink...")
    if debug:
        Writer.write_to_console(stg_df)

    # write to staging which will trigger function to properly insert into prod table  # noqa E501
    # This is done to address the issue of having duplicate values
    logger.info("Writing to sink..")
    sink_df = Writer.write_to_sink(stg_df, interval_str="2 seconds")
    sink_df.awaitTermination(timeout=30)
    logger.info("App stopped due to timeout")


if __name__ == "__main__":
    sys.exit(main())
