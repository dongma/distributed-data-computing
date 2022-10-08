# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession


def get_spark_context() -> SparkSession:
    """用于创建spark session对象"""
    builder = SparkSession.builder.appName("pandas-on-spark")\
        .master("local[*]")
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")  # bugfix in graphframe_0.8.1 version
    # Pandas API on Spark automatically uses this Spark session with the configurations set.
    return builder.getOrCreate()