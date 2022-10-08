//-- Load Node data from social-nodes.csv
LOAD CSV WITH HEADERS FROM "file:/data/social-nodes.csv" AS row
merge (place:User {id:row.id})

//-- Load relation data from social-relationships.csv
LOAD CSV WITH HEADERS FROM "file:/data/social-relationships.csv" AS row
match(source:User {id: row.src})
match(destination:User {id: row.dst})
merge(source)-[:FOLLOWS]->(destination)

//-- #1.使用neo4j实现接近中心性算法，u为节点，n是与u位于同一分量
call algo.closeness.stream("User", "FOLLOWS")
yield nodeId, centrality
return algo.getNodeById(nodeId).id, centrality
order by centrality desc

//-- #2.接近中心性算法变体：Wassemrman & Faust算法
call algo.closeness.stream("User", "FOLLOWS", {improved: true})
yield nodeId, centrality
return algo.getNodeById(nodeId) as user, centrality
order by centrality desc

// #3.使用neo4j实现调和中心性算法, 当处理联通分量时，可以使用此语句
call algo.closeness.harmonic.stream("User", "FOLLOWS")
yield nodeId, centrality
return algo.getNodeById(nodeId).id as user, centrality
order by centrality desc

// #4.中间中心性算法，用neo4j实现该算法，识别在"组织中有影响力的人"
call algo.betweenness.stream("User", "FOLLOWS")
yield nodeId, centrality
return algo.getNodeById(nodeId).id as user, centrality
order by centrality desc

// #5.使用neo4j实现pagerank算法，"Doug"是其中最具有影响力的用户，"Mark"紧随其后
call algo.pageRank.stream('User', 'FOLLOWS', {iterations:20, dampingFactor:0.84})
yield nodeId, score
return algo.getNodeById(nodeId).id as page, score
order by score desc