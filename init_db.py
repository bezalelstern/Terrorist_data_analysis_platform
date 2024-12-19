from neo4j import GraphDatabase

neo4j_driver = None


def init_neo4j():
    neo4j_driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password")
    )
    return neo4j_driver
