
class AnalyseRepository:
    def __init__(self, driver):
        self.driver = driver

    def Find_most_lethal_attacktypes(self, limit=1000000):
        query = """
        MATCH (g:Group)-[a:ATTACKED]->(l:Location)
        WITH a.attack AS attack_type, 
        SUM(COALESCE(a.dead, 0) * 2 + COALESCE(a.injured, 0)) AS total_score
        ORDER BY total_score DESC
        LIMIT %s
        RETURN attack_type, total_score
        """ % limit

        with self.driver.session() as session:
            results = session.run(query)

            return list(results)


    def Find_avj_victims_per_event(self, limit=1000000):
        query = """
        MATCH (g:Group)-[a:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
        WITH r.name AS region, 
        COUNT(a) AS num_events,
        SUM(COALESCE(a.killed, 0) * 2 + COALESCE(a.injured, 0)) AS total_victims
        RETURN region, 
        total_victims / num_events AS avg_victims_per_event,
        a.longitude as longitude,
        a.latitude as latitude
        ORDER BY avg_victims_per_event DESC
        LIMIT %s
        """ % limit

        with self.driver.session() as session:
            results = session.run(query)

            return list(results)

    def Find_most_victims_per_grop(self, limit=1000000):
        query = """
            MATCH (g:Group)-[a:ATTACKED]->(l:Location)
            WHERE a.dead IS NOT NULL OR a.injured IS NOT NULL
            WITH a.target AS targetType, 
            SUM(COALESCE(a.dead, 0) + COALESCE(a.injured, 0)) AS totalCasualties
            RETURN targetType, totalCasualties
            ORDER BY totalCasualties DESC
        LIMIT %s
        """ % limit

        with self.driver.session() as session:
            results = session.run(query)

            return list(results)

    def get_active_groups_by_region(self,region):
        query = """
        MATCH (g:Group)-[:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
        WHERE r.name = $region
        WITH g.name AS group_name, COUNT(*) AS event_count
        RETURN group_name, event_count
        ORDER BY event_count DESC
        """
        with self.driver.session() as session:
            result = session.run(query, region=region)
            return result.data()
