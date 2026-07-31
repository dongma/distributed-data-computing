# 基于Spark与Neo4j的分布式图数据分析

这个项目整理了学习分布式计算和图数据库过程中的一些实践，包括Hadoop、Hive、Spark以及Neo4j的使用。

## 技术栈

### 基础设施

- **Hadoop**: 分布式计算平台，核心是HDFS和MapReduce，用来存数据和跑计算任务
- **Docker**: 用来快速搭环境，省得手动配置

```bash
# 启动Hadoop
docker run -p 50070:50070 -p 9000:9000 -p 8088:8088 -it sequenceiq/hadoop-docker /etc/bootstrap.sh -bash
```

### 数据存储

- **Neo4j**: 图数据库，用来存图结构的数据，配合APOC和Graph Algorithm插件挺好用的

```bash
# 启动Neo4j
docker run -d --name neo4j_db -p 7474:7474 -p 7687:7687 \
  -v /tmp/neo4j/data:/data -v /tmp/neo4j/logs:/logs \
  -v /tmp/neo4j/conf:/var/lib/neo4j/conf \
  -v /tmp/neo4j/import:/var/lib/neo4j/import \
  -v /tmp/neo4j/plugins:/plugins \
  --env NEO4J_AUTH=neo4j/password neo4j
```

- **Hive**: 基于Hadoop的数据仓库，用SQL查询挺方便的

### 计算框架

- **Apache Spark 3.0**: 分布式计算引擎
- **PySpark + GraphFrames 0.8**: Python版的图计算
- **Databricks**: 定制Spark SQL扩展

## 主要内容

### 图数据导入

把CSV格式的节点和边数据导入GraphFrames和Neo4j。数据存在`dataset`文件夹下，导入Neo4j后的效果：

<img src="dataset/doc_reference/neo4j_transport_data.jpg" alt="neo4j_transport_data" width="90%" align=center />

### 图算法

这部分是学习的重点，包括：

#### 中心性算法（Centrality）

| 算法 | 作用 |
|------|------|
| 度中心性 | 数一下每个节点有多少条边连出去 |
| 接近中心性 | 看一个节点到其他节点的平均距离远近 |
| 中间中心性 | 找出网络中的"桥梁"节点 |
| PageRank | 评估节点的重要性，类似谷歌那个 |

#### 社区发现算法（Community Detection）

| 算法 | 作用 |
|------|------|
| 三角形计数 | 统计形成三角形的节点 |
| 聚类系数 | 衡量邻居节点之间是否也互相认识 |
| 强连通分量（SCC） | 找出相互都能到达的节点集合 |
| 标签传播 | 通过标签传播来划分社区 |
| Louvain | 优化模块度来发现社区 |

#### 路径查找算法（Path Finding）

| 算法 | 作用 |
|------|------|
| 最短路径 | 找两点之间的最短路 |
| A*算法 | 带启发式的路径搜索 |
| K最短路径 | 找出前K条最短路径 |
| 最小生成树 | 用最小权重连接所有节点 |
| 随机游走 | 随机遍历图的路径 |

### 数据分析

#### Hive SQL电影数据分析

用的是`hive_sql_test1`库，有t_user（6000+用户）、t_movie（3000+电影）、t_rating（100万+评分）三张表。做了些有意思的分析：

- 按年龄段统计某部电影的评分
- 找出男性评分最高的10部电影

#### Spark SQL优化规则

学习Catalyst优化器时自己捣鼓的，包括：

- CombineFilters：合并过滤条件
- CollapseProject：折叠投影
- BooleanSimplification：简化布尔表达式
- ConstantFolding：常量折叠
- PushDownPredicates：谓词下推
- ReplaceDistinctWithAggregate：去重转聚合
- ReplaceExceptWithAntiJoin：EXCEPT转反连接

#### 航空网络分析

基于美国航班真实数据做的分析：

- 看芝加哥ORD这种枢纽机场的延误情况
- 算各家航空公司的航线覆盖
- 找异常航班模式

## 各模块说明

### hadoop-mapreduce

一些MapReduce的Java示例代码。

### hive-sql

Hive SQL的练习，主要是电影数据分析，具体见[hive-sql/README.md](hive-sql/README.md)。

### databricks-app

学习Spark SQL时自己扩展的东西，实现了自定义命令和优化规则，具体见[databricks-app/README.md](databricks-app/README.md)。

### neo4j_analytics

用Cypher实现的图算法，按功能分了三块：

- `centrality/`: 度中心性
- `community/`: 社区发现（三角计数、聚类系数、SCC、标签传播、Louvain）
- `path_finding/`: 路径查找（最短路径、A*、K最短路径、最小生成树、随机游走）

### pyspark_graph

用GraphFrames实现的图计算，涵盖了中心性、社区发现、最短路径这些算法，还有简单的图机器学习。

### yelp_airport_analysis

航空数据分析的练习：

- `airport_discovery.py`: 美国机场航线网络分析
- `yelp_explore.py`: Neo4j图探索

## 数据集

### 交通网络

欧洲主要城市之间的公路连接，含距离信息。

### 社交网络

模拟的用户关注关系。

### 软件依赖

编程库之间的依赖关系。

### 航空数据

美国航班记录，包括延误时间、飞行距离等。

## 快速开始

### 环境

- Java 8+
- Python 3.7+
- Docker
- Spark 3.0+
- Neo4j 4.x

### 运行

```python
from pyspark_graph.initial_spark import get_spark_context
from pyspark_graph.import_graph import create_transport_graph

spark = get_spark_context()
graph = create_transport_graph(spark)

# 算度中心性
degrees = graph.outDegrees
```

Neo4j里跑算法：

```cypher
CALL algo.closeness.stream('Place', 'EROAD')
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).id, centrality
ORDER BY centrality DESC
```
