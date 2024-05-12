import os
from configparser import ConfigParser
from typing import Optional

from pyspark import SparkConf

CONFIG_FILE = 'app.conf'

# ensures config file path is located


def find(name: str, path: str) -> Optional[str]:
    """
    Recursively search for a file with the given name in the directory tree
    rooted at the given path,

    Parameters
    ----------
    name : str
        The name of the file to search for
    path : str
        The path to the directory to start the search from

    Returns
    -------
    str or None
        The full path to the file if found, otherwise None
    """
    for root, _, files in os.walk(path):
        if name in files:
            file_path = os.path.join(root, name)
            # print(file_path)
            return file_path
    return None


# file = "./workdir/app/conf/app.conf"
def get_app_conf(file_name: str, section_name: str) -> dict:
    """
    Reads the given section from the config file and returns its key-value pairs

    Parameters
    ----------
    file_name : str
        The name of the config file
    section_name : str
        The name of the section to read from the config file

    Returns
    -------
    dict
        The key-value pairs of the section

    Raises
    ------
    Exception
        If the config file is not found
    """
    file = find(file_name, os.getcwd())
    if file is None:
        raise Exception("Config file not found..")
    
    config = ConfigParser()
    config.read(file)
    conf = {}
    for key, val in config.items(section_name):
        conf[key] = val
    return conf






def get_kafka_conf() -> dict:
    """
    Reads the KAFKA section from the config file and returns its key-value pairs

    Returns
    -------
    dict
        The key-value pairs of the KAFKA section

    Raises
    ------
    Exception
        If the config file is not found
    """
    return get_app_conf(file_name=CONFIG_FILE, section_name="KAFKA")





def get_db_conf() -> dict:
    """
    Reads the POSTGRES section from the config file and returns its key-value pairs

    Returns
    -------
    dict
        The key-value pairs of the POSTGRES section

    Raises
    ------
    Exception
        If the config file is not found
    """
    return get_app_conf(file_name=CONFIG_FILE, section_name="POSTGRES")


def get_sr_conf() -> dict:
    """
    Reads the SCHEMA_REG section from the config file and returns its key-value pairs

    Returns
    -------
    dict
        The key-value pairs of the SCHEMA_REG section

    Raises
    ------
    Exception
        If the config file is not found
    """
    return get_app_conf(file_name=CONFIG_FILE, section_name="SCHEMA_REG")


def get_spark_conf(file_name: str, env: str = "SPARK") -> SparkConf:
    """
    Reads the given section from the config file and sets the key-value pairs as Spark config properties

    Parameters
    ----------
    file_name : str
        The name of the config file
    env : str
        The name of the section to read from the config file (default: "SPARK")

    Returns
    -------
    SparkConf
        The Spark configuration

    Raises
    ------
    Exception
        If the config file is not found
    """
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
