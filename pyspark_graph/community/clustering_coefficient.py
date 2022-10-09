# -*- coding:utf-8 -*-
from graphframes import GraphFrame
from pyspark.sql.session import SparkSession

from pyspark_graph.import_graph import create_software_graph
from pyspark_graph.initial_spark import get_spark_context
from pyspark.sql import functions as F


def triangleCount(graph: GraphFrame) -> None:
	"""用于graph中各节点的三角计数"""
	# +-----+---------------+
	# | count | id |
	# +-----+---------------+
	# | 1 	| ipykernel |
	# | 1 	| six |
	# | 1 	| python - dateutil |, triangleCount为三角计数函数，但spark无法获取三角形流s
	result = graph.triangleCount()
	result.sort("count", ascending=False).filter('count > 0')\
		.show()


def stronglyConnected(graph: GraphFrame) -> None:
	"""用于计算强连通分量，会返回每个'节点'->'分量id',图中任意两点之间需双向可达"""
	# | component 	| libraries |
	# +-------------+-----------------+
	# | 180388626432 | [jpy - core] |
	# | 223338299392 | [spacy] |
	# | 498216206336 | [numpy] |
	# | 523986010112 | [six] |
	# | 549755813888 | [pandas] |
	# | 558345748480 | [nbconvert] |, 强联通component为分组的id
	result = graph.stronglyConnectedComponents(maxIter=10)
	result.sort("component").groupby("component").agg(F.collect_list("id").alias("libraries"))\
		.show(truncate=False)


def connectedComponents(graph: GraphFrame) -> None:
	"""图中节点单向联通，弱连通算法，只要节点之前存在一条边就可以"""
	result = graph.connectedComponents()
	result.sort("component").groupby("component").agg(F.collect_list("id").alias("libraries"))\
		.show(truncate=False)


def labelPropagation(graph: GraphFrame) -> None:
	"""标签传播算法，常用于发现大规模网络中的初始社团"""
	# +-------------+----------------------------------+
# | label | collect_list(id) |
# +-------------+----------------------------------+
# | 549755813888 | [six, spacy, pandas, matplotlib] |
# | 764504178688 | [nbconvert, ipykernel, jpy - client] |
# | 833223655424 | [numpy, pytz, python - dateutil] |
# | 936302870528 | [pyspark] |
# | 1099511627776 | [jpy - core, jpy - console, jupyter] |
# | 1279900254208 | [py4j] |
# +-------------+----------------------------------+
	result = graph.labelPropagation(maxIter=10)
	result.sort("label").groupby("label").agg(F.collect_list("id")).show(truncate=False)


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	# Please set it first using sc.setCheckpointDir() problem问题，
	spark.sparkContext.setCheckpointDir("checkpoint-data")
	software_graph = create_software_graph(spark)
	# triangleCount(software_graph)

	# stronglyConnected(software_graph)

	# | component | libraries |
	# +------------+------------------------------------------------------------------+
	# | 180388626432 | [jpy - core, nbconvert, ipykernel, jupyter, jpy - client, jpy - console] |
	# | 223338299392 | [spacy, numpy, six, pandas, pytz, python - dateutil, matplotlib] |
	# | 936302870528 | [pyspark, py4j] |
	# +------------+-------------------, 弱连通只要有一条边连接，节点就会被分在同一组中
	# connectedComponents(software_graph)

	# 标签传播算法，也是用来设置区分subgraph子图内容
	labelPropagation(software_graph)
