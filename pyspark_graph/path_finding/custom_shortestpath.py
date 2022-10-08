# -*- coding:utf-8 -*-
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import functions as F, DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark_graph.import_graph import create_transport_graph
from pyspark_graph.initial_spark import get_spark_context

add_path_udf = F.udf(lambda path, id: path + [id], ArrayType(StringType()))


def sssp(graph: GraphFrame, origin, column_name="cost") -> DataFrame:
	"""用graphframe实现单元最短路径, origin为起始点"""
	vertices = graph.vertices.withColumn("visited", F.lit(False)) \
		.withColumn("distance", F.when(graph.vertices["id"] == origin, 0).otherwise(float("inf"))) \
		.withColumn("path", F.array())
	cached_vertices = AM.getCachedDataFrame(vertices)
	g2 = GraphFrame(cached_vertices, graph.edges)

	while g2.vertices.filter('visited == False').first():
		current_node_id = g2.vertices.filter('visited == False') \
			.sort("distance").first().id
		msg_distance = AM.edge[column_name] + AM.src['distance']
		msg_path = add_path_udf(AM.src["path"], AM.src["id"])
		msg_for_dst = F.when(AM.src["id"] == current_node_id, F.struct(msg_distance, msg_path))

		new_distances = g2.aggregateMessages(F.min(AM.msg).alias("aggMess"),
											 sendToDst=msg_for_dst)
		new_visited_col = F.when(
			g2.vertices.visited | (g2.vertices.id == current_node_id), True) \
			.otherwise(False)
		new_distance_col = F.when(new_distances["aggMess"].isNotNull()
								  & (new_distances.aggMess["col1"] < g2.vertices.distance),
								  new_distances.aggMess["col1"]) \
			.otherwise(g2.vertices.distance)
		new_path_col = F.when(new_distances["aggMess"].isNotNull() &
							  (new_distances.aggMess["col1"] < g2.vertices.distance),
							  new_distances.aggMess["col2"].cast("array<string>")).otherwise(g2.vertices.path)

		new_vertices = g2.vertices.join(new_distances, on="id", how="left_outer").drop(new_distances["id"]) \
			.withColumn("visited", new_visited_col).withColumn("newDistance", new_distance_col) \
			.withColumn("newPath", new_path_col).drop("aggMess", "distance", "path") \
			.withColumnRenamed('newDistance', 'distance') \
			.withColumnRenamed('newPath', 'path')
		cached_vertices = AM.getCachedDataFrame(new_vertices)
		g2 = GraphFrame(cached_vertices, g2.edges)

	return g2.vertices.withColumn("newPath", add_path_udf("path", "id")).drop("visited", "path") \
		.withColumnRenamed("newPath", "path")


via_udf = F.udf(lambda path: path[1:-1], ArrayType(StringType()))

if __name__ == '__main__':
	# | id 					| distance 	| via 			|
	# +----------------+--------+-------------------------------------------------------------+
	# | Amsterdam 			| 0.0 		| [] 			|
	# | Utrecht 			| 46.0 		| [] 			|
	# | Den Haag 			| 59.0 		| [] 			|
	# | Gouda 				| 81.0 		| [Utrecht] 	|
	# | Rotterdam 			| 85.0 		| [Den Haag] 	|
	# | Hoek van Holland 	| 86.0 		| [Den Haag] 	|
	spark: SparkSession = get_spark_context()
	g: GraphFrame = create_transport_graph(spark)
	result = sssp(g, "Amsterdam", "cost")
	result.withColumn("via", via_udf("path")).select("id", "distance", "via")\
		.sort("distance").show(truncate=False)
