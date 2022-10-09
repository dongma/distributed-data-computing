//-- Load Node data from social-nodes.csv
LOAD CSV WITH HEADERS FROM "file:/data/sw-nodes.csv" AS row
merge (:Library {id:row.id})

//-- Load relation data from social-relationships.csv
LOAD CSV WITH HEADERS FROM "file:/data/sw-relationships.csv" AS row
match(source:Library {id: row.src})
match(destination:Library {id: row.dst})
merge(source)-[:DEPENDS_ON]->(destination)

// #1. 使用Neo4j实现三角形计数算法，并把形成三角形的节点打印出来(还为6个点，但是是3组)
call algo.triangle.stream("Library", "DEPENDS_ON")
yield nodeA, nodeB, nodeC
return algo.getNodeById(nodeA).id as nodeA,
  algo.getNodeById(nodeB).id as nodeB, algo.getNodeById(nodeC).id as nodeC

// #2. 使用Neo4j计算局部聚类系数，并按系数指标进行排序; 系数越大，意味着它所有邻节点都彼此相邻
call algo.triangleCount.stream("Library", "DEPENDS_ON")
yield nodeId, triangles, coefficient
where coefficient > 0
return algo.getNodeById(nodeId).id as library, coefficient
order by coefficient desc

// #3. 使用Neo4j实现强连通分量算法，其也会根据partition进行分组, 与graphframe结果一致
call algo.scc.stream("Library", "DEPENDS_ON")
yield nodeId, partition
return partition, collect(algo.getNodeById(nodeId)) as libraries
order by size(libraries) desc

// #4. 测试，临时添加一个extra的库，它在py4j和pyspark之间创建了循环依赖关系，再次执行后，在一个分区了
//│"partition"│"libraries"                                    │
//╞═══════════╪═══════════════════════════════════════════════╡
//│0          │[{"id":"extra"},{"id":"pyspark"},{"id":"py4j"}]
match (py4j:Library {id: "py4j"})
match (pyspark:Library {id: "pyspark"})
merge (extra:Library {id: "extra"})
merge (py4j)-[:DEPENDS_ON]->(extra) merge (extra)-[:DEPENDS_ON]->(pyspark)
// 删除图中extra库及其关系，介绍接下来的另一个算法
match(extra: Library {id: "extra"}) detach delete extra

// #5. 使用neo4j实现连通分量算法，setId为连通分量的分组id
call algo.unionFind.stream("Library", "DEPENDS_ON")
yield nodeId, setId
return setId, collect(algo.getNodeById(nodeId)) as libraries
order by size(libraries) desc

// #6. 使用Neo4j实现标签传播算法，其中label相当于"分组"的id编号，{direction: "BOTH"}选项用于"忽略方向"
call algo.labelPropagation.stream("Library", "DEPENDS_ON", {iterations: 10, direction: "BOTH"})
yield nodeId, label
return label, collect(algo.getNodeById(nodeId).id) as libraries
order by size(libraries) desc

// #7. 使用Neo4j实现Louvain模块算法 (apoc此算法未查出数据)
call algo.louvain.stream("Library", "DEPENDS_ON")
yield nodeId, communities
return algo.getNodeById(nodeId).id as libraries, communities

// 使用此算法流版本来存储产生的社团，之后调用Set子句来存储结果，用子查询查找出此数据
call algo.louvain.stream("Library", "DEPENDS_ON")
yield nodeId, communities
with algo.getNodeById(nodeId) As node, communities
set node.communities = communities

// l.communities[-1]返回存储与底层数组中的最后一项 (底层数据存在问题)
match(l:Library)
return l.communities[-1] as community, collect(l.id) as libraries
order by size(libraries) desc
