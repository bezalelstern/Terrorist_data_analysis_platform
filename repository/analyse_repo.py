
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
        if region == "all":
            query = """
            MATCH (r:Region)<-[:PART_OF]-(c:Country)<-[:IN_COUNTRY]-(l:Location)<-[:ATTACKED]-(g:Group)
            WITH r.name AS region_name, g.name AS group_name, COUNT(*) AS event_count
            ORDER BY event_count DESC
            WITH region_name, group_name, event_count
            WITH region_name, COLLECT({group_name: group_name, event_count: event_count})[0..5] AS top_groups
            RETURN region_name, top_groups
            """
            params = {}

            with self.driver.session() as session:
                results = session.run(query, params)
                data = []
                for record in results:
                    data.append({
                        "region_name": record["region_name"],
                        "top_groups": [
                            {
                                "group_name": tg["group_name"],
                                "event_count": tg["event_count"]
                            }
                            for tg in record["top_groups"]
                        ]
                    })
                return data

        else:
            query = """
            MATCH (g:Group)-[:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
            WHERE r.name = $region
            WITH g.name AS group_name, COUNT(*) AS event_count, collect(l.name) AS locations
            ORDER BY event_count DESC
            RETURN g.name as group_name, event_count, locations, r.name as region_name
            """
            params = {'region': region}

            with self.driver.session() as session:
                results = session.run(query, params)
                # עבור אזור בודד, נחזיר מילון אחד עם רשימת קבוצות.
                top_groups = []
                region_name = None

                for record in results:
                    region_name = record["region_name"]
                    top_groups.append({
                        "group_name": record["group_name"],
                        "event_count": record["event_count"],
                        "locations": record["locations"],
                    })

                top_groups = sorted(top_groups, key=lambda x: x["event_count"], reverse=True)

                # בניית המבנה הסופי
                data = []
                if region_name:
                    data.append({
                        "region_name": region_name,
                        "top_groups": top_groups
                    })
                return data

    def get_influential_groups(self, region):
        query = """
                MATCH (g:Group)-[a:ATTACKED]->(l:Location)-[:IN_COUNTRY]->(c:Country)-[:PART_OF]->(r:Region)
                WHERE ($region IS NULL OR r.name = $region)
                WITH g.name AS group_name, COUNT(DISTINCT r) AS region_links, COUNT(DISTINCT a.target) AS target_links
                RETURN group_name, region_links + target_links AS total_links
                ORDER BY total_links DESC
                LIMIT 10
        """
        params = {'region': region}
        print("get_influential_groups")
        with self.driver.session() as session:
            result = session.run(query, params)
            print([dict(record) for record in result])
            return [dict(record) for record in result]