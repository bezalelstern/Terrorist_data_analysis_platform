import math
from datetime import datetime
from flask import current_app as app
from neo4j import GraphDatabase
import pandas as pd

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


def create_graph(tx, group, attack, region, country, location, target, date, killed, injured, latitude, longitude):
    killed = 0 if math.isnan(killed) else killed
    injured = 0 if math.isnan(injured) else injured
    longitude = 0 if math.isnan(longitude) else longitude
    latitude = 0 if math.isnan(latitude) else latitude
    query = """
    MERGE (g:Group {name: $group})
    MERGE (r:Region {name: $region})
    MERGE (c:Country {name: $country})
    MERGE (l:Location {name: $location})
    
    // קשרים
    MERGE (c)-[:PART_OF]->(r)
    MERGE (l)-[:IN_COUNTRY]->(c)
    MERGE (g)-[:ATTACKED {date: $date, target: $target, attack: $attack, dead: $dead, injured: $injured, latitude: $latitude, longitude: $longitude}]->(l)
    """
    tx.run(query, group=group, attack=attack, region=region, country=country, location=location, target=target,
           date=date, dead=killed, injured=injured, latitude = latitude, longitude = longitude )


file_path = "../data/globalterrorismdb_0718dist-1000 rows.csv"
data = pd.read_csv(file_path, encoding="latin1")

# בחירת עמודות רלוונטיות
columns_of_interest = [
    "gname", "attacktype1_txt", "region_txt", "country_txt", "city", "targtype1_txt", "iyear", "imonth", "iday", "nkill", "nwound", "latitude", "longitude"
]
data = data[columns_of_interest]

with driver.session() as session:
    for _, row in data.iterrows():
        if pd.isna(row["iyear"]) or pd.isna(row["imonth"]) or pd.isna(row["iday"]) | row["imonth"] not in range(1,13) or row["iday"] <1:
            date = "unknown"
        else:
            date = datetime(row["iyear"], row["imonth"], row["iday"])


        session.execute_write(
            create_graph,
            row["gname"],
            row["attacktype1_txt"],
            row["region_txt"],
            row["country_txt"],
            row["city"],
            row["targtype1_txt"],
            date,
            row["nkill"],
            row["nwound"],
            row["latitude"],
            row["longitude"]
        )



driver.close()
print("הנתונים הוזנו בהצלחה לגרף.")

driver.close()