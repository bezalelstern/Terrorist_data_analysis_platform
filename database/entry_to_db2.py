import math
import pandas as pd
from datetime import datetime
from flask import current_app as app


def create_constraints(driver):
    with driver.session() as session:
        session.run("CREATE CONSTRAINT group_name IF NOT EXISTS FOR (g:Group) REQUIRE g.name IS UNIQUE")
        session.run("CREATE CONSTRAINT region_name IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE")
        session.run("CREATE CONSTRAINT country_name IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE")
        session.run("CREATE CONSTRAINT location_name IF NOT EXISTS FOR (l:Location) REQUIRE l.name IS UNIQUE")
    print("Constraints created (or already exist).")



def create_graph(tx, group, attack, region, country, location, target, date, killed, injured, latitude, longitude):
    killed = 0 if (pd.isna(killed) or math.isnan(killed)) else killed
    injured = 0 if (pd.isna(injured) or math.isnan(injured)) else injured
    longitude = 0 if (pd.isna(longitude) or math.isnan(longitude)) else longitude
    latitude = 0 if (pd.isna(latitude) or math.isnan(latitude)) else latitude


    if pd.isna(group) or not group:
        group = "Unknown"
    if pd.isna(attack) or not attack:
        attack = "Unknown"
    if pd.isna(region) or not region:
        region = "Unknown"
    if pd.isna(country) or not country:
        country = "Unknown"
    if pd.isna(location) or not location:
        location = "Unknown"
    if pd.isna(target) or not target:
        target = "Unknown"


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

    tx.run(
        query,
        group=group,
        attack=attack,
        region=region,
        country=country,
        location=location,
        target=target,
        date=date_str,
        dead=killed,
        injured=injured,
        latitude=latitude,
        longitude=longitude
    )


def batch_insert(driver, data, batch_size=10000):

    total_rows = len(data)
    print(f"Starting batch insert for {total_rows} rows with batch_size={batch_size}...")


    for start_idx in range(0, total_rows, batch_size):
        end_idx = start_idx + batch_size
        batch_df = data.iloc[start_idx:end_idx]
        print(f"Inserting batch from row {start_idx} to {end_idx}...")


        with driver.session() as session:
            with session.begin_transaction() as tx:
                for _, row in batch_df.iterrows():

                    if (pd.isna(row["iyear"]) or pd.isna(row["imonth"]) or pd.isna(row["iday"])
                            or row["imonth"] not in range(1, 13) or row["iday"] < 1):
                        date_value = "unknown"
                    else:
                        try:
                            date_value = datetime(int(row["iyear"]), int(row["imonth"]), int(row["iday"]))
                        except ValueError:
                            date_value = "unknown"

                    create_graph(
                        tx,
                        row["gname"],
                        row["attacktype1_txt"],
                        row["region_txt"],
                        row["country_txt"],
                        row["city"],
                        row["targtype1_txt"],
                        date_value,
                        row["nkill"],
                        row["nwound"],
                        row["latitude"],
                        row["longitude"]
                    )


    print("Finished batch insert.")


def read_and_save_neo4j():
    with app.app_context():
        file_path = "../data/globalterrorismdb_0718dist.csv"
        try:
            data = pd.read_csv(file_path, encoding="latin1")
        except Exception as e:
            print(f"Error reading the file: {e}")
            return

        columns_of_interest = [
            "gname",
            "attacktype1_txt",
            "region_txt",
            "country_txt",
            "city",
            "targtype1_txt",
            "iyear",
            "imonth",
            "iday",
            "nkill",
            "nwound",
            "latitude",
            "longitude"
        ]
        data = data[columns_of_interest]

        create_constraints(app.driver)

        try:
            batch_insert(app.driver, data, batch_size=10000)
            print("Data loaded successfully into Neo4j.")
        except Exception as e:
            print(f"An error occurred while loading data into Neo4j: {e}")
