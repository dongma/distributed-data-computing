# -*- coding:utf-8 -*-
import pandas as pd
from neo4j import GraphDatabase
from tabulate import tabulate
import matplotlib

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


def show_node_types(driver) -> None:
	"""从neo4j中查找所有节点类型，并通过tabulate进行展示"""
	result = {"label": [], "count": []}
	with driver.session() as session:
		labels = [row["label"] for row in session.run("CAll db.labels()")]
		for label in labels:
			query = f"Match (:`{label}`) return count(*) as count"
			count = session.run(query).single()["count"]
			result["label"].append(label)
			result["count"].append(count)
	# +---------+---------+
	# | label 	| count |
	# | ---------+--------- |
	# | User 	| 9 |
	# | Place 	| 12 |
	# | Library | 15 |
	# +---------+---------+
	df = pd.DataFrame(data=result)
	print(tabulate(df.sort_values("count"), headers='keys', tablefmt='psql', showindex=False))
	plt.style.use('fivethirtyeight')  # 将数据进行可示化
	ax = df.plot(kind='bar', x='label', y='count', legend=None)
	ax.xaxis.set_label_text("")
	plt.yscale("log")
	plt.xticks(rotation=45)
	plt.tight_layout()
	# plt.show()


def show_relation_types(driver) -> None:
	"""从neo4j中查找所有关系类型，并通过tabulate进行展示"""
	# +------------+---------+
	# | relType | count |
	# | ------------+--------- |
	# | EROAD | 15 |
	# | FOLLOWS | 16 |
	# | DEPENDS_ON | 18 |
	# | MINST | 22 |
	# +------------+---------+
	result = {"relType": [], "count": []}
	with driver.session() as session:
		rel_types = [row["relationshipType"] for row in session.run("call db.relationshipTypes()")]
		for rel_type in rel_types:
			query = f"MATCH ()-[:`{rel_type}`]->() return count(*) as count"
			count = session.run(query).single()["count"]
			result["relType"].append(rel_type)
			result["count"].append(count)
	df = pd.DataFrame(data=result)
	print(tabulate(df.sort_values("count"), headers='keys', tablefmt='psql', showindex=False))


if __name__ == '__main__':
	driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo4j#2022"))
	# 先研究关于节点和关系的常规数值，先汇总下neo4j中各类型节点的数量
	show_node_types(driver)
	show_relation_types(driver)
