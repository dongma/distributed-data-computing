# -*- coding:utf-8 -*-
from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pyspark_graph.initial_spark import get_spark_context
from tabulate import tabulate
import matplotlib

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


def build_airports_graph(spark: SparkSession) -> GraphFrame:
	"""加载飞机场、航线的csv数据，并生成飞机航行线路的图谱"""
	nodes = spark.read.csv("dataset/airports.csv", header=False)
	cleaned_nodes = (nodes.select("_c1", "_c3", "_c4", "_c6", "_c7").filter("_c3 == 'United States'")
					 .withColumnRenamed("_c1", "name").withColumnRenamed("_c4", "id")
					 .withColumnRenamed("_c6", "latitude").withColumnRenamed("_c7", "longitude").drop("_c3"))
	cleaned_nodes = cleaned_nodes[cleaned_nodes["id"] != "\\N"]

	relationships = spark.read.csv("dataset/188591317_T_ONTIME.csv", header=True)
	cleaned_relationships = (relationships.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY", "DISTANCE",
												  "TAIL_NUM", "FL_NUM", "CRS_DEP_TIME", "CRS_ARR_TIME",
												  "UNIQUE_CARRIER")
							 .withColumnRenamed("ORIGIN", "src").withColumnRenamed("DEST", "dst")
							 .withColumnRenamed("DEP_DELAY", "deptDelay").withColumnRenamed("ARR_DELAY", "arrDelay")
							 .withColumnRenamed("TAIL_NUM", "tailNumber").withColumnRenamed("FL_NUM", "flightNumber")
							 .withColumnRenamed("FL_DATE", "date").withColumnRenamed("CRS_DEP_TIME", "time")
							 .withColumnRenamed("CRS_ARR_TIME", "arrivalTime")
							 .withColumnRenamed("DISTANCE", "distance").withColumnRenamed("UNIQUE_CARRIER", "airline")
							 .withColumn("deptDelay", F.col("deptDelay").cast(FloatType()))
							 .withColumn("arrDelay", F.col("arrDelay").cast(FloatType()))
							 .withColumn("time", F.col("time").cast(IntegerType()))
							 .withColumn("arrivalTime", F.col("arrivalTime").cast(IntegerType()))
							 )
	graph = GraphFrame(cleaned_nodes, cleaned_relationships)
	return graph


def loadAirlinesData(spark: SparkSession) -> Column:
	"""从csv中加载航空公司的数据"""
	airlines = (spark.read.csv("dataset/airlines.csv").select("_c1", "_c3")
				.withColumnRenamed("_c1", "name").withColumnRenamed("_c3", "code"))
	airlines = airlines[airlines["code"] != "null"]
	return airlines


def searchDelayFromORD(graph: GraphFrame) -> None:
	"""了解像芝加哥ORD机场中间枢纽所导致的延误"""
	delayed_flights = (graph.edges.filter("src = 'ORD' and deptDelay > 0").groupBy("dst")
					   .agg(F.avg("deptDelay"), F.count("deptDelay")).withColumn("averageDelay",
																				 F.round(F.col("avg(deptDelay)"), 2))
					   .withColumn("numberOfDelays", F.col("count(deptDelay)")))
	# 按目的机场分组计算延迟后，可将spark生成的DataFrame对象与airplanes进行连接操作
	(delayed_flights.join(graph.vertices, delayed_flights.dst == graph.vertices.id)
	 .sort(F.desc("averageDelay")).select("dst", "name", "averageDelay", "numberOfDelays")
	 .show(n=10, truncate=False))


def bad_day_inSFO(graph: GraphFrame) -> None:
	"""旧金山机场(SFO)的航班延误，机场因大雾导致低能见度的问题，研究模体(motif)"""
	motifs = (graph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
			  .filter("""(b.id = 'SFO') and (ab.date = '2018-05-11' and bc.date = '2018-05-11') and 
			  	(ab.arrDelay > 30 or bc.deptDelay > 30) and
			  	(ab.flightNumber = bc.flightNumber) and (ab.airline = bc.airline) and (ab.time < bc.time)
			  """))
	result = (motifs.withColumn("delta", motifs.bc.deptDelay - motifs.ab.arrDelay)
			  .select("ab", "bc", "delta").sort("delta", ascending=False))
	# 得到结果并选择感兴趣的列展示，包括航班的开始节点src
	# | airline | flightNumber | a1 | a1DeptTime | arrDelay | a2 | a2DeptTime | deptDelay | a3 | delta |
	# +-------+------------+---+----------+--------+---+----------+---------+---+-----+
	# | WN | 1454 | PDX | 1130 | -18.0 | SFO | 1350 | 178.0 | BUR | 196.0 |
	# | OO | 5700 | ACV | 1755 | -9.0 | SFO | 2235 | 64.0 | RDM | 73.0 |
	# | UA | 753 | BWI | 700 | -3.0 | SFO | 1125 | 49.0 | IAD | 52.0 |
	# | UA | 1900 | ATL | 740 | 40.0 | SFO | 1110 | 77.0 | SAN | 37.0 |
	result.select(F.col("ab.airline"),
				  F.col("ab.flightNumber"),
				  F.col("ab.src").alias("a1"),
				  F.col("ab.time").alias("a1DeptTime"),
				  F.col("ab.arrDelay"),
				  F.col("ab.dst").alias("a2"),
				  F.col("bc.time").alias("a2DeptTime"),
				  F.col("bc.deptDelay"),
				  F.col("bc.dst").alias("a3"),
				  F.col("delta")).show()


def find_connected_airports(graph: GraphFrame, airline_reference: Column) -> DataFrame:
	"""获取到通过航空公司互连的机场，强度连通算法scc的实际使用"""
	airlines = (graph.edges.groupBy("airline").agg(F.count("airline").alias("flights"))
				.sort("flights", ascending=False))
	full_name_airlines = (airline_reference.join(
		airlines, airlines.airline == airline_reference.code).select("code", "name", "flights"))
	full_name_airlines.show(truncate=False)
	return full_name_airlines


def find_scc_components(graph: GraphFrame, airline) -> DataFrame:
	"""创建只包含指定航班的子图，通过airline过滤数据边"""
	airline_relations = graph.edges[graph.edges.airline == airline]
	airline_graph = GraphFrame(graph.vertices, airline_relations)
	# Calculate the strongly connected Components
	scc = airline_graph.stronglyConnectedComponents(maxIter=10)
	# 查找最大分量的大小并返回
	return (scc.groupBy("component")
		.agg(F.count("id").alias("size")).sort("size", ascending=False)
		.tail(1)[0]["size"])


def airline_sccs(spark: SparkSession, graph: GraphFrame, airlines: Column,
				 full_airlines: DataFrame):
	"""DataFrame对象包含每家航空公司及其最大强连通分量中的机场数量"""
	airline_scc = [(airline, find_scc_components(graph, airline))
				   for airline in airlines.toPandas()["airline"].tolist()]
	airline_scc_df = spark.createDataFrame(airline_scc, ['id', 'sccCount'])
	# 对DataFrame对象scc和airlines进行连接运算，获取航空公司的航班数量
	(airline_scc_df.join(full_airlines, full_airlines.code == airline_scc_df.id)
	 .select("code", "name", "flights", "sccCount")
	 .sort("sccCount", ascending=False)) \
		.show(truncate=False)


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	airport_graph = build_airports_graph(spark)
	# 先进行探索性分析，了解机场的数量及连接边的数量
	print(f"airport size: {airport_graph.vertices.count()}, edge size: {airport_graph.edges.count()}")
	# 1.哪些机场的出港航班最多，可使用中心性算法计算数量，选择出度最大的节点
	airports_degree = airport_graph.outDegrees.withColumnRenamed("id", "oId")
	full_airports_degree = (airports_degree
							.join(airport_graph.vertices, airports_degree.oId == airport_graph.vertices.id)
							.sort("outDegree", ascending=False).select("id", "name", "outDegree"))
	# full_airports_degree.show(n=10, truncate=False)

	# 将航班数据进行可视化，按航班数量展示出从Den到Atl的出港航班数量(Matplotlib)
	# plt.style.use('fivethirtyeight')
	# ax = (full_airports_degree.toPandas().head(10).plot(kind='bar', x='id', y='outDegree', legend=None))
	# ax.xaxis.set_label_text("")
	# plt.xticks(rotation=45)
	# plt.tight_layout()
	# plt.show()

	# 2.返回航班延误最严重的10个目的机场，用到了spark#RDD之间的join操作
	# searchDelayFromORD(graph=airport_graph)
	# SFO（旧金山）的糟糕一天，因大雾导致的低能见度的问题
	# bad_day_inSFO(airport_graph)

	# 3.获取航空公司互连的机场
	reference_airlines = loadAirlinesData(spark)
	full_airlines = find_connected_airports(airport_graph, reference_airlines)

	# +----+---------------------------+-------+--------+
	# | code | name | flights | sccCount |
	# +----+---------------------------+-------+--------+
	# | F9 | Frontier Airlines | 10297 | 1 |
	# | OO | SkyWest | 65157 | 1 |
	# 4.计算每家航空公司的最大联通分量，数据可能存在一定问题，to be fixed.
	airlines = (airport_graph.edges.groupBy("airline").agg(F.count("airline").alias("flights"))
				.sort("flights", ascending=False))
	airline_sccs(spark, airport_graph, airlines, full_airlines)
