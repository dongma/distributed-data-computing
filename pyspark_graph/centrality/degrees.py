# -*- coding:utf-8 -*-
from graphframes import GraphFrame
from pyspark.sql.session import SparkSession

from pyspark_graph.initial_spark import get_spark_context
from pyspark_graph.import_graph import create_social_graph
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import functions as F
from pyspark.sql.types import *
from operator import itemgetter


def collect_paths(paths):
	return F.collect_set(paths)


collect_paths_udf = F.udf(collect_paths, ArrayType(StringType()))
paths_type = ArrayType(
	StructType([StructField("id", StringType()), StructField("distance", LongType())]))


def flatten(ids):
	flat_list = [item for sublist in ids for item in sublist]
	return list(dict(sorted(flat_list, key=itemgetter(0))).items())


flatten_udf = F.udf(flatten, paths_type)


def new_paths(paths, id):
	paths = [{"id": col1, "distance": col2 + 1} for col1, col2 in paths if col1 != id]
	paths.append({"id": id, "distance": 1})
	return paths


new_paths_udf = F.udf(new_paths, paths_type)


def merge_paths(ids, new_ids, id):
	joined_ids = ids + (new_ids if new_ids else [])
	merged_ids = [(col1, col2) for col1, col2 in joined_ids if col1 != id]
	best_ids = dict(sorted(merged_ids, key=itemgetter(1), reverse=True))
	return [{"id": col1, "distance": col2} for col1, col2 in best_ids.items()]


merge_paths_udf = F.udf(merge_paths, paths_type)


def calculate_closeness(ids):
	nodes = len(ids)
	total_distance = sum([col2 for col1, col2 in ids])
	return 0 if total_distance == 0 else nodes * 1.0 / total_distance


closeness_udf = F.udf(calculate_closeness, DoubleType())


def closeness_centrality(graph: GraphFrame) -> None:
	"""接近中心性算法，计算每个节点的得分值"""
	vertices = graph.vertices.withColumn("ids", F.array())
	cached_vertices = AM.getCachedDataFrame(vertices)
	g2 = GraphFrame(cached_vertices, graph.edges)

	for i in range(0, g2.vertices.count()):
		msg_dst = new_paths_udf(AM.src["ids"], AM.src["id"])
		msg_src = new_paths_udf(AM.dst["ids"], AM.dst["id"])
		agg = g2.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
								   sendToSrc=msg_src, sendToDst=msg_dst)
		res = agg.withColumn("newIds", flatten_udf("agg")).drop("agg")
		new_vertices = (g2.vertices.join(res, on="id", how="left_outer")
						.withColumn("mergedIds", merge_paths_udf("ids", "newIds", "id")).drop("ids", "newIds")
						.withColumnRenamed("mergedIds", "ids"))
		cached_new_vertices = AM.getCachedDataFrame(new_vertices)
		g2 = GraphFrame(cached_new_vertices, g2.edges)
	# 对中心性centrality数据进行展示
	(g2.vertices.withColumn("closeness", closeness_udf("ids"))
	 .sort("closeness", ascending=False)
	 .show(truncate=False))


def degrees_centrality(graph: GraphFrame) -> None:
	"""度中心性算法：total_degree总度数、in_degree和out_degree：入度和出度，并按入度对所有节点排序"""
	total_degree = graph.degrees
	in_degree = graph.inDegrees
	out_degree = graph.outDegrees
	# total_degree和out_degree是按id进行"join"的，fillna(0)排出null记录，需再仔细斟酌下
	# | id 		| degree 	| inDegree 	| outDegree |
	# +-------+------+--------+---------+
	# | Doug 	| 	6 		| 	5 		| 	1 	|
	# | Alice 	| 	7 		| 	3 		| 	4 	|
	# | Bridget | 	5 		| 	2 		| 	3 	|
	# | Michael | 	5 		| 	2 		| 	3 	|
	(total_degree.join(in_degree, "id", how="left")
	 .join(out_degree, "id", how="left").fillna(0)
	 .sort("inDegree", ascending=False)
	 .show())


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	social_graph: GraphFrame = create_social_graph(spark)
	degrees_centrality(social_graph)
	# 对graph图谱中的所有节点执行接近中心性算法，并展示结果
	# +-------+-----------------------------------------------------------------+------------------+
	# | id 		| ids | closeness |
	# +-------+-----------------------------------------------------------------+------------------+
	# | Doug 	| [{Charles, 1}, {Mark, 1}, {Alice, 1}, {Bridget, 1}, {Michael, 1}] | 1.0 |
	# | Alice 	| [{Charles, 1}, {Mark, 1}, {Bridget, 1}, {Doug, 1}, {Michael, 1}] | 1.0 |
	# | David 	| [{James, 1}, {Amy, 1}] 	| 1.0 |
	# | Bridget | [{Charles, 2}, {Mark, 2}, {Alice, 1}, {Doug, 1}, {Michael, 1}] | 0.7142857142857143 |
	# closeness_centrality(social_graph)

	# graphframes支持两种PageRank算法实现，第1种通过设置maxIter参数实现；第2种设置tol参数，直至收敛
	# +-------+-------------------+
	# | id | pagerank |
	# +-------+-------------------+
	# | Doug | 2.2865372087512252 |
	# | Mark | 2.1424484186137263 |
	# | Alice | 1.520330830262095 |
	results = social_graph.pageRank(resetProbability=0.15, maxIter=20)
	results.vertices.sort("pagerank", ascending=False).show()

	# +-------+-------------------+
	# | id 		| pagerank |
	# +-------+-------------------+
	# | Doug 	| 2.2233188859989745 |
	# | Mark 	| 2.090451188336932 |
	# | Alice 	| 1.5056291439101062 |
	# | Michael | 0.733738785109624 |
	results = social_graph.pageRank(resetProbability=0.15, tol=0.01)
	results.vertices.sort("pagerank", ascending=False).show()