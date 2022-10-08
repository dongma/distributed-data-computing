# -*- coding:utf-8 -*-
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import functions as F
from pyspark_graph.import_graph import create_transport_graph
from pyspark_graph.initial_spark import get_spark_context


add_path_udf = F.udf(lambda path, id: path + [id], ArrayType(StringType()))


def shortest_path(g: GraphFrame, origin, destination, spark: SparkSession,
                  column_name="cost") -> DataFrame:
    """shortest_path用于从源节点开始计算最短路径，访问到目标后，就立即返回."""
    sc = spark.sparkContext
    if g.vertices.filter(g.vertices.id == destination).count() == 0:
        return (spark.createDataFrame(sc.emptyRDD(), g.vertices.schema)
                .withColumn("path", F.array()))

    # 对vertices节点添加{"visited", "distance", "path"}属性，默认visited为false，当访问过节点后，visited为true
    vertices = (g.vertices.withColumn("visited", F.lit(False))
                .withColumn("distance", F.when(g.vertices["id"] == origin, 0).otherwise(float("inf")))
                .withColumn("path", F.array()))
    cached_vertices = AM.getCachedDataFrame(vertices)
    g2 = GraphFrame(cached_vertices, g.edges)

    # 当graph中所有节点都已被访问时，结束BSP发消息，结束计算模型
    while g2.vertices.filter('visited == False').first():
        current_node_id = g2.vertices.filter('visited == False')\
            .sort("distance").first().id

        msg_distance = AM.edge[column_name] + AM.src['distance']
        msg_path = add_path_udf(AM.src["path"], AM.src["id"])
        # 组装要发给dst的消息，消息结构包括struct(msg_distance, msg_path)内容，F.min(AM.msg)会过滤取distance最小的消息
        msg_for_dst = F.when(AM.src['id'] == current_node_id, F.struct(msg_distance, msg_path))
        new_distances = g2.aggregateMessages(F.min(AM.msg).alias('aggMess'),
                                             sendToDst=msg_for_dst)

        new_visited_col = F.when(g2.vertices.visited | (g2.vertices.id == current_node_id), True)\
            .otherwise(False)
        new_distance_col = F.when(new_distances["aggMess"].isNotNull() &
                                  (new_distances.aggMess["col1"] < g2.vertices.distance), new_distances.aggMess["col1"])\
            .otherwise(g2.vertices.distance)
        new_path_col = F.when(new_distances["aggMess"].isNotNull() &
                              (new_distances.aggMess["col1"] < g2.vertices.distance), new_distances.aggMess['col2'].cast("array<string>"))\
            .otherwise(g2.vertices.path)
        new_vertices = (g2.vertices.join(new_distances, on="id", how="left_outer").drop(new_distances["id"])
                        .withColumn("visited", new_visited_col)
                        .withColumn("newDistance", new_distance_col)
                        .withColumn("newPath", new_path_col).drop("aggMess", "distance", "path")
                        .withColumnRenamed("newDistance", "distance").withColumnRenamed("newPath", 'path'))
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)

        g2 = GraphFrame(cached_new_vertices, g2.edges)
        if g2.vertices.filter(g2.vertices.id == destination).first().visited:
            return (g2.vertices.filter(g2.vertices.id == destination)
                    .withColumn("newPath", add_path_udf("path", "id"))
                    .drop("visited", "path")
                    .withColumnRenamed("newPath", "path"))
    return (spark.createDataFrame(sc.emptyRDD(), g.vertices.schema)
            .withColumn("path", F.array()))


if __name__ == '__main__':
    spark: SparkSession = get_spark_context()
    graph = create_transport_graph(spark)
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

    # +----------+--------+------------------------------------------------------------------------+
    # | id | distance | path |
    # +----------+--------+------------------------------------------------------------------------+
    # | Colchester | 347.0 | [Amsterdam, Den Haag, Hoek van Holland, Felixstowe, Ipswich, Colchester] |
    # +----------+--------+------------------------------------------------------------------------+
    # 计算两个地点间的最短路径，"Amsterdam"和"Colchester"之间的距离，并打印出距两地间的距离
    result = shortest_path(graph, "Amsterdam", "Colchester", spark, "cost")
    result.select("id", "distance", "path").show(truncate=False)

    # +----------------+--------------------------------------------------------+
    # | id | distances |
    # +----------------+--------------------------------------------------------+
    # | Amsterdam | {Immingham -> 1, Hoek van Holland -> 2, Colchester -> 4} |
    # | Colchester| {Hoek van Holland -> 3, Immingham -> 3, Colchester -> 0} |
    # 使用shortestPaths查找所有点到target节点集合的最短路径，["Colchester", "Immingham", "Hoek van Holland"]
    # origin_graph = create_transport_graph(spark)
    sssp_result = graph.shortestPaths(["Colchester", "Immingham", "Hoek van Holland"])
    sssp_result.sort(["id"]).select("id", "distances").show(truncate=False)
