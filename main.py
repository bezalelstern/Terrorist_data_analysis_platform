from datetime import datetime

from neo4j import GraphDatabase
import pandas as pd

file_path = "data/globalterrorismdb_0718dist-1000 rows.csv"
data = pd.read_csv(file_path, encoding="latin1")

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


def create_graph(tx, group, attack, region, country, location, target, date):
    query = """
    // יצירת קבוצת הטרור
    MERGE (g:Group {name: $group})

    // יצירת האזור והמדינה
    MERGE (r:Region {name: $region})
    MERGE (c:Country {name: $country})
    MERGE (c)-[:PART_OF]->(r)

    // יצירת המיקום (עיר)
    MERGE (l:Location {name: $location})
    
    // קשרים
    MERGE (l)-[:IN_COUNTRY]->(c)
    MERGE (g)-[:ATTACKED {date: $date, target: $target, attack: $attack}]->(l)
    """
    tx.run(query, group=group, attack=attack, region=region, country=country, location=location, target=target,
           date=date)


file_path = "data/globalterrorismdb_0718dist-1000 rows.csv"
data = pd.read_csv(file_path, encoding="latin1")

# בחירת עמודות רלוונטיות
columns_of_interest = [
    "gname", "attacktype1_txt", "region_txt", "country_txt", "city", "targtype1_txt", "iyear", "imonth", "iday"
]
data = data[columns_of_interest]

# הכנסת נתונים לגרף
with driver.session() as session:
    for _, row in data.iterrows():
        if pd.isna(row["iyear"]) or pd.isna(row["imonth"]) or pd.isna(row["iday"]) | row["imonth"] not in range(1,13) or row["iday"] <1:
            date = "unknown"
        else:
            print(row["iday"])
            date = datetime(row["iyear"], row["imonth"], row["iday"])
        session.execute_write(
            create_graph,
            row["gname"],
            row["attacktype1_txt"],
            row["region_txt"],
            row["country_txt"],
            row["city"],
            row["targtype1_txt"],
            date
        )



# סגירת החיבור
driver.close()
print("הנתונים הוזנו בהצלחה לגרף.")

driver.close()