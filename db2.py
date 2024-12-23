import math
from datetime import datetime
from neo4j import GraphDatabase
import pandas as pd

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


def create_graph(tx, group, attack, region, country, location, target, date, killed, injured, latitude, longitude):

    # Handle NaN values for numeric properties
    killed = 0 if (pd.isna(killed) or math.isnan(killed)) else killed
    injured = 0 if (pd.isna(injured) or math.isnan(injured)) else injured
    longitude = 0 if (pd.isna(longitude) or math.isnan(longitude)) else longitude
    latitude = 0 if (pd.isna(latitude) or math.isnan(latitude)) else latitude

    # Handle NaN or missing string fields
    if pd.isna(group):
        group = "Unknown"
    if pd.isna(attack):
        attack = "Unknown"
    if pd.isna(region):
        region = "Unknown"
    if pd.isna(country):
        country = "Unknown"
    if pd.isna(location):
        location = "Unknown"
    if pd.isna(target):
        target = "Unknown"

    # Correctly format date as string for Neo4j
    if isinstance(date, datetime):
        date_str = date.isoformat()
    else:
        date_str = "unknown"

    query = """
    MERGE (g:Group {name: $group})
    MERGE (r:Region {name: $region})
    MERGE (c:Country {name: $country})
    MERGE (l:Location {name: $location})
    MERGE (c)-[:PART_OF]->(r)
    MERGE (l)-[:IN_COUNTRY]->(c)
    MERGE (g)-[:ATTACKED {
        date: $date, 
        target: $target, 
        attack: $attack, 
        dead: $dead, 
        injured: $injured, 
        latitude: $latitude, 
        longitude: $longitude
    }]->(l)
    """
    tx.run(query, group=group, attack=attack, region=region, country=country, location=location, target=target,
           date=date_str, dead=killed, injured=injured, latitude=latitude, longitude=longitude)


file_path = "data/globalterrorismdb_0718dist.csv"
try:
    data = pd.read_csv(file_path, encoding="latin1")
except FileNotFoundError:
    print(f"Error: The file {file_path} was not found. Please check the path")
    exit()
except Exception as e:
    print(f"Error reading the file: {e}")
    exit()

# Select relevant columns
columns_of_interest = [
    "gname", "attacktype1_txt", "region_txt", "country_txt", "city",
    "targtype1_txt", "iyear", "imonth", "iday", "nkill", "nwound", "latitude", "longitude"
]
data = data[columns_of_interest].copy()

# Insert data into Neo4j
try:
    with driver.session() as session:
        for _, row in data.iterrows():
            # Handle date parsing
            if (pd.isna(row["iyear"]) or pd.isna(row["imonth"]) or pd.isna(row["iday"])
                    or row["imonth"] not in range(1, 13) or row["iday"] < 1):
                date = "unknown"
            else:
                try:
                    date = datetime(int(row["iyear"]), int(row["imonth"]), int(row["iday"]))
                except ValueError:
                    date = "unknown"

            # Execute Neo4j operation
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
    print("Data loaded successfully into Neo4j.")
except Exception as e:
    print(f"An error occurred while loading data into Neo4j: {e}")
finally:
    driver.close()
