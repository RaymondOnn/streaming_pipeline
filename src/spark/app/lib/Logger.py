from pyspark.sql import SparkSession


class Log4j:
    def __init__(self, spark: SparkSession) -> None:
        """
        Initialize the Log4j logger with the Spark application name.

        Parameters
        ----------
        spark : SparkSession
            The Spark application session

        Returns
        -------
        None
        """
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        log4j = spark._jvm.org.apache.log4j # type: ignore
        self.logger = log4j.LogManager.getLogger(app_name) # type: ignore

    def warn(self, message: str) -> None:
        """
        Log a message at the WARN level.

        Parameters:
            message (str): The message to log.

        Returns:
            None
        """
        self.logger.warn(message)

    def info(self, message: str) -> None:
        """
        Log a message at the INFO level.

        Parameters
        ----------
        message : str
            The message to log

        Returns
        -------
        None
        """
        self.logger.info(message)

    def error(self, message: str) -> None:
        """
        Log a message at the ERROR level.

        Parameters
        ----------
        message : str
            The message to log

        Returns
        -------
        None
        """
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """
        Log a message at the DEBUG level.

        Parameters
        ----------
        message : str
            The message to log

        Returns
        -------
        None
        """
        self.logger.debug(message)
