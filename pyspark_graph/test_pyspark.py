# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession

from graphframes import GraphFrame

if __name__ == '__main__':
	# conf = pyspark.SparkConf().setMaster("local[*]").setAppName("PySparkTest")
	# sc = pyspark.SparkContext(conf=conf)

	# words = ["hello", "world", "python", "hello", "hello"]
	# rdd = sc.parallelize(words)
	# counts = rdd.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
	# print(counts.collect())

	import findspark
	findspark.init()
	# 测试graphframes框架，检测当前环境是否可行，终于环境跑通过了，在坑里面呆了2天了（得写一篇blog记录下）
	pyspark = SparkSession.builder.appName("Graphframe").master("local[*]").getOrCreate()
	# Vertex DataFrame
	vertexList = pyspark.createDataFrame([
		("a", "Alice", 34),
		("b", "Bob", 36),
		("c", "Charlie", 30),
		("d", "David", 29)
	], ["id", "name", "age"])
	# Edge DataFrame
	edgeList = pyspark.createDataFrame([
		("a", "b", "friend"),
		("b", "c", "follow"),
		("c", "b", "follow")
	], ["src", "dst", "relationship"])
	# Create a GraphFrame
	g = GraphFrame(vertexList, edgeList)

	from graphframes.examples import Graphs
	g = Graphs(pyspark).friends()

	from graphframes.examples import Graphs
	g = Graphs(pyspark).friends()  # Get example graph
	# Display the vertex and edge DataFrames
	g.vertices.show()
	g.edges.show()