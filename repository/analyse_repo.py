
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

            return [dict(record) for record in results]


    def Find_avj_victims_per_event(self, limit=1000000):
        query = """
            MATCH (g:Group)-[a:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
            WITH r.name AS region, 
                 COUNT(a) AS num_events,
                 SUM(COALESCE(a.killed, 0) * 2 + COALESCE(a.injured, 0)) AS total_victims
            RETURN region, 
                   total_victims / num_events AS avg_victims_per_event
            ORDER BY avg_victims_per_event DESC
            LIMIT $limit
            """

        params = {'limit': limit}

        with self.driver.session() as session:
            results = session.run(query, params)
            return [dict(record) for record in results]

    def find_avg_victims_per_region(self, top_regions=5):
        query = """
            MATCH (g:Group)-[a:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
            WITH r.name AS region, 
                 COUNT(a) AS num_events,
                 SUM(COALESCE(a.dead, 0) * 2 + COALESCE(a.injured, 0)) AS total_victims
            RETURN region, 
                   total_victims / num_events AS avg_victims_per_event
            ORDER BY avg_victims_per_event DESC
            LIMIT $top_regions
        """

        params = { 'top_regions': top_regions}

        with self.driver.session() as session:
            results = session.run(query, params)
            return [dict(record) for record in results]



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

            return [dict(record) for record in results]

    def get_active_groups_by_region(self, region):
        if region.lower() == "all":
            query = """
            MATCH (r:Region)<-[:PART_OF]-(c:Country)<-[:IN_COUNTRY]-(l:Location)<-[:ATTACKED]-(g:Group)
            WITH r.name AS region_name, g.name AS group_name, COUNT(*) AS event_count
            ORDER BY event_count DESC
            WITH region_name, group_name, event_count
            WITH region_name, COLLECT({group_name: group_name, event_count: event_count})[0..5] AS top_groups
            RETURN region_name, top_groups
            """
            params = {}
        else:
            query = """
            MATCH (g:Group)-[:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
            WHERE r.name = $region
            WITH g.name AS group_name, COUNT(*) AS event_count, collect(l.name) AS locations
            RETURN group_name, event_count, locations
            ORDER BY event_count DESC
            """
            params = {'region': region}

        with self.driver.session() as session:
            result = session.run(query, params)
            return [dict(record) for record in result]

    def get_influential_groups(self, region, country= None):
        query = """
        MATCH (g:Group)-[:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
        OPTIONAL MATCH (g)-[:TARGETED]->(t:Target)
        WHERE ($region IS NULL OR r.name = $region)
        WITH g.name AS group_name, COUNT(DISTINCT r) AS region_links, COUNT(DISTINCT t) AS target_links
        RETURN group_name, region_links + target_links AS total_links
        ORDER BY total_links DESC
        LIMIT 5
        """
        params = {'region': region}

        with self.driver.session() as session:
            result = session.run(query, params)
            return [dict(record) for record in result]