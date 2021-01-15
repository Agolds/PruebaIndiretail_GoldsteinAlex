from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#TODO: agregar logging


class SparkUtil:
    def __init__(self):
        print("config logging")

    @staticmethod
    def get_spark_context(
            app_name=None, spark_session_config_dict=None, hive_session=False
    ):
        """
        :param spark_session_config_dict: additional spark configurations
        :param app_name: application name need to be given to spark context
        :param conf_dict: configuration needed to build the spark session
        :param hive_session: check this to True, if need hive session enabling
        :return: spark session
        """

        config_dict = {
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.shuffle.partitions": "2001",
        }

        if spark_session_config_dict is not None:
            for key, value in spark_session_config_dict.items():
                config_dict[key] = value

        spark_builder = SparkSession.builder
        spark_builder = (
            spark_builder.appName(app_name) if (app_name is not None) else spark_builder
        )

        for key, value in config_dict.items():
            spark_builder = spark_builder.config(key, value)

        return (
            spark_builder.enableHiveSupport() if hive_session else spark_builder
        ).getOrCreate()

    @staticmethod
    def save_data(
            df,
            output_path,
            save_format="parquet",
            save_mode="append",
            partitions_cols=None,
            repartition_needed=True,
            no_of_partitions=None,
            custom_partitions_cols=None,
            replace_where=None,
    ):
        """
        spark dataframe to save at path location
        :param df: spark dataframe (spark dataframe to save at output path)
        :param output_path: str (location to save the data)
        :param save_format: str (format to save the data like parquet, json, etc)
        :param save_mode: str (spark mode to save the data like append, overwrite, etc)
        :param partitions_cols: list(str) (physical partitions to create for the data)
        :param repartition_needed: bool (set this to true, if repartition needed)
        :param no_of_partitions: int (number of physical files needed to create at output location)
        :param custom_partitions_cols: list(str, str) (if needed any explicit physical partitions with some custom value
                                        (specify (column name, constant column value))
        :param replace_where: str (condition for overwriting specific partitions)
        :return: None
        """

        print(" Spark Util: %s", save_format)

        if custom_partitions_cols is not None:
            for (col_name, col_value) in custom_partitions_cols.items():
                df = df.withColumn(col_name, lit(col_value))
                partitions_cols.insert(0, col_name)

        print("Final schema:")
        df.printSchema()
        if no_of_partitions is not None:
            df = df.repartition(no_of_partitions)
        elif repartition_needed and partitions_cols and len(partitions_cols) > 0:
            df = df.repartition(*partitions_cols)

        df_to_save = df.write
        if save_mode:
            df_to_save = df_to_save.mode(save_mode)
        if partitions_cols is not None and len(partitions_cols) > 0:
            df_to_save = df_to_save.partitionBy(*partitions_cols)
        if save_format:
            df_to_save = df_to_save.format(save_format).option("mergeSchema", "True")
        if replace_where:
            df_to_save = df_to_save.option("replaceWhere", replace_where)

        df_to_save.save(output_path)

    @staticmethod
    def read_data(
            spark_session,
            source_path,
            file_format,
            infer_schema=True,
            header=False
    ):
        """
        Load data from source path
        :param infer_schema: set to True if spark should infer origin schema
        :param file_format: file's origin format
        :param spark_session: object pointer Spark
        :param source_path: file's origin path
        :param header: set to True if first row in data is header
        :return: Dataframe
        """

        df = spark_session.read\
            .format(file_format)\
            .option("header", header)\
            .option("inferSchema", infer_schema)\
            .load(source_path)

        return df
