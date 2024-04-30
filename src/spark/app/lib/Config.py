import os
from configparser import ConfigParser

from pyspark import SparkConf

CONFIG_FILE = 'app.conf'

# ensures config file path is located
def find(name, path):
    for root, _, files in os.walk(path):
        if name in files:
            # print(root)
            # print(os.path.join(root, name))
            return os.path.join(root, name)


# file = "./workdir/app/conf/app.conf"
def get_app_conf(file_name: str, section_name: str):
    file = find(file_name, os.getcwd())
    if file is None:
        raise Exception("Config file not found..")
    
    config = ConfigParser()
    config.read(file)
    conf = {}
    for key, val in config.items(section_name):
        conf[key] = val
    return conf



def get_kafka_conf():
    return get_app_conf(file_name=CONFIG_FILE, section_name="KAFKA")


def get_db_conf():
    return get_app_conf(file_name=CONFIG_FILE, section_name="POSTGRES")


def get_sr_conf():
    return get_app_conf(file_name=CONFIG_FILE, section_name="SCHEMA_REG")


def get_spark_conf(file_name: str, env: str = "SPARK"):
    file = find(file_name, os.getcwd())
    if file is None:
        raise Exception("Config file not found..")
    
    spark_conf = SparkConf()
    config = ConfigParser()
    config.read(file)

    for key, val in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


# def get_data_filter(env, data_filter):
#     conf = get_config(env)
#     return "true" if conf[data_filter] == "" else conf[data_filter]


if __name__ == "__main__":
    print(get_db_conf().items())
