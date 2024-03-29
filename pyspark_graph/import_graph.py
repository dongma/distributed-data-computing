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


def create_social_graph(spark: SparkSession) -> GraphFrame:
    """加载社交social边和点的csv，并创建GraphFrame对象"""
    vertex = spark.read.csv("dataset/social-nodes.csv", header=True)
    edge = spark.read.csv("dataset/social-relationships.csv", header=True)
    return GraphFrame(vertex, edge)


def create_software_graph(spark: SparkSession):
    """示例数据：软件依赖图，python中各个依赖库的关系"""
    nodes = spark.read.csv("dataset/sw-nodes.csv", header=True)
    relationships = spark.read.csv("dataset/sw-relationships.csv", header=True)
    return GraphFrame(nodes, relationships)
