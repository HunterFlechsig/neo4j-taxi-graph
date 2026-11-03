from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        drop_query = """
            CALL gds.graph.drop('bfs_graph', false)
        """

        proj_query = """
            MATCH (source:Location)-[r:TRIP]->(target:Location)
            RETURN gds.graph.project(
                'bfs_graph',
                source,
                target
            )
        """

        dfs_query = """
            MATCH (source:Location {name: $start_node}),
                (destination:Location {name: $last_node})

            CALL gds.bfs.stream('bfs_graph', {
                sourceNode: source,
                targetNodes: [destination]
            })
            YIELD path

            RETURN path
        """

        self._driver.execute_query(
            drop_query,
            database_="neo4j"
        )

        self._driver.execute_query(
            proj_query,
            database_="neo4j"
        )

        records, _, _ = self._driver.execute_query(
            query_=dfs_query, 
            start_node=start_node,
            last_node=last_node,
            database_="neo4j"
        )

        return records
    
    def pagerank(self, max_iterations, weight_property):
        drop_query = """
            CALL gds.graph.drop('pr_graph', false)
        """

        proj_query = """
            MATCH (source:Location)-[r:TRIP]->(target:Location)
            RETURN gds.graph.project(
                'pr_graph',
                source,
                target,
                {
                    relationshipProperties: {
                        distance: r.distance,
                        fare: r.fare
                    }
                }
            )
        """

        pagerank_query = """
            CALL gds.pageRank.stream(
                'pr_graph',
                {
                    maxIterations: $max_iterations,
                    dampingFactor: 0.85,
                    relationshipWeightProperty: $weight_property
                }
            )
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).name AS name, score
            ORDER BY score DESC
        """

        self._driver.execute_query(
            drop_query,
            database_="neo4j"
        )

        self._driver.execute_query(
            proj_query,
            database_="neo4j"
        )

        records, _, _ = self._driver.execute_query(
            query_=pagerank_query, 
            max_iterations=max_iterations,
            weight_property=weight_property,
            database_="neo4j"
        )

        return[records[0].data(), records[-1].data()]

