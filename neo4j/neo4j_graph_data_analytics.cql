-- Load Node data from transport-nodes.csv
LOAD CSV WITH HEADERS FROM "file:/var/lib/neo4j/import/data/transport-nodes.csv" AS row
merge (place:Place {id:row.id})
set place.latitude = toFloat(row.latitude),
place.longitude = toFloat(row.longitude),
place.population = toInteger(row.population)

-- Load relation data from transport-relationships.csv
LOAD CSV WITH HEADERS FROM "file:/var/lib/neo4j/import/data/transport-relationships.csv" AS row
match(origin:Place {id: row.src})
match(destination:Place {id: row.dst})
merge(origin)-[:EROAD {distance: toInteger(row.cost)}]->(destination)
