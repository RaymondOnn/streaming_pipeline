import os
from configparser import ConfigParser

from pyspark import SparkConf


# ensures config file path is located
def find(name, path):
    for root, _, files in os.walk(path):
        if name in files:
            # print(root)
            # print(os.path.join(root, name))
            return os.path.join(root, name)


# file = "./workdir/app/conf/app.conf"
def get_app_conf(env: str):
    file = find("app.conf", os.getcwd())
    config = ConfigParser()
    config.read(file)
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf


def get_kafka_conf():
    return get_app_conf("KAFKA")


def get_db_conf():
    return get_app_conf("POSTGRES")
    # return {
    #     'user': 'postgres',
    #     'password': 'postgres',
    #     'db_name': 'postgres',
    #     'server': 'postgresdb',
    #     'port': '5432',
    #     'schema': 'staging'
    # }


def get_sr_conf():
    return get_app_conf("SCHEMA_REG")


def get_spark_conf(env: str = "SPARK"):
    spark_conf = SparkConf()
    config = ConfigParser()
    file = find("app.conf", os.getcwd())
    config.read(file)

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


# def get_data_filter(env, data_filter):
#     conf = get_config(env)
#     return "true" if conf[data_filter] == "" else conf[data_filter]


if __name__ == "__main__":
    print(get_db_conf().items())
