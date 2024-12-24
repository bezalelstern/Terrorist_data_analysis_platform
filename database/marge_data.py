import math
from datetime import datetime
from flask import current_app as app
import pandas as pd
from opencage.geocoder import OpenCageGeocode

API_KEY = "38a6b2d679e845cebdfacedb8e58ea74"
geocoder = OpenCageGeocode(API_KEY)

def get_coordinates_from_address(address):
    if address is None:
        return 0, 0
    result = geocoder.geocode(address)
    if result and len(result):
        location = result[0]['geometry']
        return location['lat'], location['lng']
    else:
        return 0, 0

def entry_data(tx, group, attack, country, location, target, date, killed, injured, latitude, longitude):
    killed = 0 if (pd.isna(killed) or math.isnan(killed)) else killed
    injured = 0 if (pd.isna(injured) or math.isnan(injured)) else injured
    longitude = 0 if (pd.isna(longitude) or math.isnan(longitude)) else longitude
    latitude = 0 if (pd.isna(latitude) or math.isnan(latitude)) else latitude

    group = group or "Unknown"
    attack = attack or "Unknown"
    country = country or "Unknown"
    location = location or "Unknown"
    target = target or "Unknown"

    if isinstance(date, datetime):
        date_str = date.isoformat()
    else:
        date_str = "unknown"

    query = """
    MERGE (g:Group {name: $group})
    MERGE (c:Country {name: $country})
    MERGE (l:Location {name: $location})
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
    tx.run(query, group=group, attack=attack, country=country, location=location, target=target,
           date=date_str, dead=killed, injured=injured, latitude=latitude, longitude=longitude)

def marge_data():
    with app.app_context():
        file_path = "../data/RAND_Database_of_Worldwide_Terrorism_Incidents - 5000 rows.csv"
        try:
            data = pd.read_csv(file_path, encoding="latin1")
            data.fillna({
                'City': 'Unknown',
                'Perpetrator': 'Unknown',
                'Weapon': 'Unknown',
                "Country": "Unknown",
                "Date": "Unknown",
            }, inplace=True)

        except Exception as e:
            print(f"Error reading the file1: {e}")
            exit()
        try:
            with app.driver.session() as session:
                for index, row in data.iterrows():
                    lat, lng = get_coordinates_from_address(row['City'])
                    session.execute_write(
                        entry_data,
                        group=row['Perpetrator'],
                        attack=row['Weapon'],
                        country=row['Country'],
                        location=row['City'],
                        target=row['Description'],
                        date=row['Date'],
                        killed=row['Fatalities'],
                        injured=row['Injuries'],
                        latitude=lat,
                        longitude=lng
                        )
                print("Data loaded successfully into Neo4j.")
        except Exception as e:
            print(f"Error reading the file2: {e}")


