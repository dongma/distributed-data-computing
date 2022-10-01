-- Load Node data from transport-nodes.csv
LOAD CSV WITH HEADERS FROM "file:/data/transport-nodes.csv" AS row
merge (place:Place {id:row.id})
set place.latitude = toFloat(row.latitude),
place.longitude = toFloat(row.longitude),
place.population = toInteger(row.population)

-- Load relation data from transport-relationships.csv
LOAD CSV WITH HEADERS FROM "file:/data/transport-relationships.csv" AS rows
match(origin:Place {id: row.src})
match(destination:Place {id: row.dst})
merge(origin)-[:EROAD {distance: toInteger(row.cost)}]->(destination)

-- #1. 用neo4j计算从Amsterdam->London的跳数(hop)，将null作为第3个参数
match(source:Place {id: 'Amsterdam'}), (dest :Place {id: 'London'})
call algo.shortestPath.stream(source, dest, null)
yield nodeId, cost
return algo.getNodeById(nodeId).id as place, cost
-- ### place   cost
-- "Amsterdam"	0.0
-- "Immingham"	1.0
-- "Doncaster"	2.0
-- "London"	3.0

-- #2. 计算从Amsterdam->London的总距离，该路线通过的城市最少，总代价为720千米，cypher语句有些复杂
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
--### place	cost
-- "Amsterdam"	0.0
-- "Immingham"	369.0
-- "Doncaster"	443.0
-- "London"	720.0

-- #3. 用Neo4j实现加权最短路径，计算从Amsterdam->London的最短路径，有权重值为453km
match(source:Place {id: 'Amsterdam'}), (dest :Place {id: 'London'})
call algo.shortestPath.stream(source, dest, "distance")
yield nodeId, cost
return algo.getNodeById(nodeId).id as place, cost
--###  place	cost
-- "Amsterdam"	0.0
-- "Den Haag"	59.0
-- "Hoek van Holland"	86.0
-- "Felixstowe"	293.0
-- "Ipswich"	315.0
-- "Colchester"	347.0
-- "London"	453.0

