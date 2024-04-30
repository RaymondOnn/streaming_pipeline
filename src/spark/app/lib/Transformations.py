from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

COL_NAMES_MAP = {
    "transaction_id": "id",
    "trans_date_trans_time": "transaction_ts",
    "trans_num": "transaction_num",
    "customer_id": "customer_id",
    "first": "first_name",
    "last": "last_name",
    "gender": "gender",
    "job": "job",
    "dob": "birth_date",
    "cc_num": "cc_num",
    "address_id": "address_id",
    "street": "street",
    "city_id": "city_id",
    "city": "city_name",
    "state": "state",
    "city_pop": "city_pop",
    "zip": "zip",
    "lat": "lat",
    "long": "long",
    "merchant_category_id": "merchant_category_id",
    "merchant_id": "merchant_id",
    "merchant": "merchant_name",
    "category_id": "category_id",
    "category": "category",
    "amt": "amount",
}

# {
#     "city": {
#         "city_id": "id",
#         "city": "city_name",
#         "state": "state",
#         "city_pop": "city_pop",
#     },
#     "address": {
#         "address_id": "id",
#         "street": "street",
#         "city_id": "city_id",
#         "zip": "zip",
#         "lat": "lat",
#         "long": "long",
#     },
#     "customer": {
#         "customer_id": "id",
#         "first": "first_name",
#         "last": "last_name",
#         "gender": "gender",
#         "job": "job",
#         "dob": "birth_date",
#         "cc_num": "cc_num",
#     },
#     "merchant": {
#         "merchant_id": "id",
#         "merchant": "merchant_name",
#     },
#     "category": {
#         "category_id": "id",
#         "category": "category",
#     },
#     "merchant_category": {
#         "merchant_category_id": "id",
#         "merchant_id": "merchant_id",
#         "category_id": "category_id",
#     },
#     "transactions": {
#         "transaction_id": "id",
#         "trans_date_trans_time": "transaction_ts",
#         "trans_num": "transaction_num",
#         "customer_id": "customer_id",
#         "merchant_category_id": "merchant_category_id",
#         "amt": "amount"
#     }
# }


# Needs some processing as
# Confluent Avro encoding not compatible with Spark's binary decoder
def decode_from_avro(df: DataFrame, schema_str: str) -> DataFrame:
    # Step 1: Create new column "fixedValue" by removing first 5 bytes
    #         added on Confluent avro format
    # Step 2: Convert avro bytes back to string
    # Step 3: Flatten 'value' column
    fromAvroOptions = {"mode": "PERMISSIVE"}
    df = (
        df.withColumn(
            "fixedValue", F.expr("substring(value, 6, length(value)-5)")
        )  # noqa E501
        .select(
            from_avro(F.col("fixedValue"), schema_str, fromAvroOptions).alias(
                "value"
            )  # noqa E501
        )
        .select("value.*")
    )
    return df


def get_col_names(table_defn: dict[str, dict[str, str]]) -> list[str]:
    cols: list[str] = []
    for _, mapping in table_defn.items():
        cols += mapping
    return list(set(cols))


def create_id_cols(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("city_id", F.expr("concat(city, state)"))
        .withColumn(
            "address_id", F.expr("create_deterministic_uuid(concat(street))")
        )  # noqa E501
        .withColumn(
            "customer_id",
            F.expr(
                "create_deterministic_uuid( \
                concat(first, last, dob, cc_num) \
            )"
            ),  # noqa E501
        )
        .withColumn(
            "merchant_id",
            F.expr("create_deterministic_uuid(concat(merchant))"),  # noqa E501
        )
        .withColumn(
            "category_id",
            F.expr("create_deterministic_uuid(concat(category))"),  # noqa E501
        )
        .withColumn(
            "merchant_category_id",
            F.expr("create_deterministic_uuid(concat(merchant, category))"),
        )
        .withColumn(
            "transaction_id",
            F.expr(
                "create_deterministic_uuid( \
                    concat(trans_num, trans_date_trans_time, amt) \
                )"
            ),
        )
    )


def choose_cols(df: DataFrame) -> DataFrame:
    return df.select(
        *[F.col(key).alias(val) for key, val in COL_NAMES_MAP.items()]
    ).withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
