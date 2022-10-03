# -*- coding:utf-8 -*-
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from graphframes import GraphFrame


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
    builder = SparkSession.builder.appName("pandas-on-spark")\
        .master("local[*]")
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
    # Pandas API on Spark automatically uses this Spark session with the configurations set.
    return builder.getOrCreate()


if __name__ == '__main__':
    graph = create_transport_graph(getSparkContext())
    # 先从所有点vertex中找到人口介于10万～30万的城市，并用show()方法展示出来
    graph.vertices.filter("population > 100000 and population < 300000") \
        .sort("population") \
        .show()
    # 从Den Haag到一个中型城市的最短路径，result.columns展示的是结果集中的列
    from_expr: str = "id='Den Haag'"
    to_expr: str = "population > 100000 and population < 300000 and id <> 'Den Haag'"
    result = graph.bfs(from_expr, to_expr)
    print(result.columns)
    # 以e开头的列代表关系(边)，而以v开头的列代表节点(顶点)，我们仅对节点感兴趣
    columns = [column for column in result.columns if not column.startswith("e")]
    result.select(columns).show()
