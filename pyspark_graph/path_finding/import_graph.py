# -*- coding:utf-8 -*-
from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, FloatType, IntegerType, StructType


def create_transport_graph(spark: SparkSession) -> GraphFrame:
    """加载nodes和边relationships的csv，创建transport graph，用union是不分区边的direct"""
    node_fields = [
        StructField("id", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("population", IntegerType(), True)
    ]
    nodes = spark.read.csv("dataset/transport-nodes.csv", header=True,
                           schema=StructType(node_fields))
    # 读取加载交通关系relationships.csv内容，并构成graphframe
    rels = spark.read.csv("dataset/transport-relationships.csv", header=True)
    reversed_rels = (rels.withColumn("newSrc", rels.dst).withColumn("newDst", rels.src) \
                     .drop("dst", "src").withColumnRenamed("newSrc", "src") \
                     .withColumnRenamed("newDst", "dst") \
                     .select("src", "dst", "relationship", "cost"))

    relationships = rels.union(reversed_rels)
    return GraphFrame(nodes, relationships)


def getSparkContext() -> SparkSession:
    """用于创建spark session对象"""
    builder = SparkSession.builder.appName("pandas-on-spark")\
        .master("local[*]")
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")  # bugfix in graphframe_0.8.1 version
    # Pandas API on Spark automatically uses this Spark session with the configurations set.
    return builder.getOrCreate()