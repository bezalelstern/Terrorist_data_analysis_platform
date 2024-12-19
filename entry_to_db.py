import math
from datetime import datetime
from flask import current_app as app
import pandas as pd


def create_graph(tx, group, attack, region, country, location, target, date, killed, injured):
    killed = 0 if math.isnan(killed) else killed
    injured = 0 if math.isnan(injured) else injured
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
    MERGE (g)-[:ATTACKED {date: $date, target: $target, attack: $attack, dead: $dead, injured: $injured}]->(l)
    """
    tx.run(query, group=group, attack=attack, region=region, country=country, location=location, target=target,
           date=date, dead=killed, injured=injured)





def read_and_save_neo4j():
    with app.app_context():
        file_path = "data/globalterrorismdb_0718dist-1000 rows.csv"
        data = pd.read_csv(file_path, encoding="latin1")

        columns_of_interest = [
            "gname", "attacktype1_txt", "region_txt", "country_txt", "city", "targtype1_txt", "iyear", "imonth", "iday",
            "nkill", "nwound"
        ]
        data = data[columns_of_interest]
        with app.driver.session() as session:
            for _, row in data.iterrows():
                row, date =chek_row(row)
                pass
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
                    row["nwound"]
                )
        print("הנתונים הוזנו בהצלחה לגרף.")

def chek_row(row):
    if pd.isna(row["iyear"]) or pd.isna(row["imonth"]) or pd.isna(row["iday"]) | row["imonth"] not in range(1,13) or \
            row["iday"] < 1:
        date = "unknown"
    else:
        date = datetime(row["iyear"], row["imonth"], row["iday"])
    return row,date