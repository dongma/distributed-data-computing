//-- Load Node data from transport-nodes.csv
LOAD CSV WITH HEADERS FROM "file:/data/transport-nodes.csv" AS row
merge (place:Place {id:row.id})
set place.latitude = toFloat(row.latitude),
place.longitude = toFloat(row.longitude),
place.population = toInteger(row.population)

//-- Load relation data from transport-relationships.csv
LOAD CSV WITH HEADERS FROM "file:/data/transport-relationships.csv" AS rows
match(origin:Place {id: row.src})
match(destination:Place {id: row.dst})
merge(origin)-[:EROAD {distance: toInteger(row.cost)}]->(destination)

//-- #1.用neo4j计算从Amsterdam->London的跳数(hop)，将null作为第3个参数
match(source:Place {id: 'Amsterdam'}), (dest :Place {id: 'London'})
call algo.shortestPath.stream(source, dest, null)
yield nodeId, cost
return algo.getNodeById(nodeId).id as place, cost
//-- ### place   cost
//-- "Amsterdam"	0.0
//-- "Immingham"	1.0
//-- "Doncaster"	2.0
//-- "London"	3.0

//-- #2.计算从Amsterdam->London的总距离，该路线通过的城市最少，总代价为720千米，cypher语句有些复杂
match(source:Place {id: 'Amsterdam'}), (dest :Place {id: 'London'})
call algo.shortestPath.stream(source, dest, null)
yield nodeId, cost

// 用collect()将从Amsterdam->London的所有点 收集起来
with collect(algo.getNodeById(nodeId)) as path
unwind range(0, size(path)-1) as index
with path[index] as current, path[index+1] as next
// 匹配current-next路径，并且取得r上distance数值
with current, next, [(current)-[r:EROAD]-(next)|r.distance][0] as distance

with collect({current:current, next:next, distance:distance}) as stops
unwind range(0, size(stops)-1) as index
with stops[index] as location, stops, index
// 这块不太懂，complex problem
return location.current.id as place,
    reduce(acc=0.0,
        distance in [stop in stops[0..index] |stop.distance] |acc + distance) as cost
//--### place	cost
//-- "Amsterdam"	0.0
//-- "Immingham"	369.0
//-- "Doncaster"	443.0
//-- "London"	720.0

//-- #3.用Neo4j实现加权最短路径，计算从Amsterdam->London的最短路径，有权重值为453km
match(source:Place {id: 'Amsterdam'}), (dest :Place {id: 'London'})
call algo.shortestPath.stream(source, dest, "distance")
yield nodeId, cost
return algo.getNodeById(nodeId).id as place, cost
//--###  place	cost
//-- "Amsterdam"	0.0
//-- "Den Haag"	59.0
//-- "Hoek van Holland"	86.0
//-- "Felixstowe"	293.0
//-- "Ipswich"	315.0
//-- "Colchester"	347.0
//-- "London"	453.0

//-- #4.使用Neo4j实现A*算法，查找Den Haag和London之间的最短路径
match (source:Place {id: "Den Haag"}), (destination:Place {id: "London"})
//-- astar.stream#参数，"distance"为边的名称，"latitude"，"longitude"为经纬度节点性质，地理空间启发结算中使用
call algo.shortestPath.astar.stream(source, destination, "distance",
"latitude", "longitude")
yield nodeId, cost
return algo.getNodeById(nodeId).id as place, cost

//-- #5.Yen的k最短路径算法，1971年提出"finding the k shortest loopless paths in a Network"
match(start:Place {id:"Gouda"}), (end:Place {id:"Felixstowe"})
//-- args说明，5表示最多查找路径数为5，"distance"表示关系边的类型
call algo.kShortestPaths.stream(start, end, 5, "distance")
yield index, nodeIds, path, costs
return index,
    [node in algo.getNodesById(nodeIds[1..-1]) | node.id] as via,
    reduce(acc=0.0, cost in costs | acc + cost) as totalCost

//-- #6.使用Neo4j实现所有点对最短路径算法，stream(type)不传参数时，表示不区分类型
call algo.allShortestPaths.stream("distance")
yield sourceNodeId, targetNodeId, distance
where sourceNodeId < targetNodeId
return algo.getNodeById(sourceNodeId).id as source, algo.getNodeById(targetNodeId).id as target, distance
order by distance desc limit 10

//-- #7.用neo4j实现单源最短路径，称为Delta-Stepping算法，将Dijkstra算法的划分多个可执行阶段
match(n:Place {id:"London"})
call algo.shortestPath.deltaStepping.stream(n, "distance", 1.0)
yield nodeId, distance
where algo.isFinite(distance)
return algo.getNodeById(nodeId).id as destination, distance
order by distance

//-- #8.用neo4j实现最小生成树算法，该查询在图上存储查询结果。"EROAD"关系类型，id(n)内部节点ID
match (n:Place {id:"Amsterdam"})
call algo.spanningTree.minimum("Place", "EROAD", "distance", id(n), {write:true, writeProperty:"MINST"})
yield loadMillis, computeMillis, writeMillis, effectiveNodeCount
return loadMillis, computeMillis, writeMillis, effectiveNodeCount

//-- 展示返回最小权重生成树，最小生成树的结果存在了图中
match path=(n:Place {id:"Amsterdam"})-[:MINST*]-()
with relationships(path) as rels
unwind rels as rel
with distinct rel as rel
return startNode(rel).id as source, endNode(rel).id as destination, rel.distance as cost

//-- #9.随机游走算法randomWalk algorithm，此算法之前一直未接触过
match(souce:Place {id:"London"})
call algo.randomWalk.stream(id(souce), 5, 1)
yield nodeIds
unwind algo.getNodesById(nodeIds) as place
return place.id as place