import math
from datetime import datetime
from flask import current_app as app
import pandas as pd

from entry_to_db import create_graph, chek_row


def read_and_save_neo4j():
    with app.app_context():
        file_path = "data/globalterrorismdb_0718dist-1000 rows.csv"
        data = pd.read_csv(file_path, encoding="latin1")

        columns_of_interest = [
            "Date", "City", "Country", "Perpetrator", "Weapon", "Injuries", "Fatalities"
        ]

        data = data[columns_of_interest]
        with app.driver.session() as session:
            for _, row in data.iterrows():
                row, date = chek_row(row)

                session.execute_write(
                    create_graph,
                    row["Perpetrator"],
                    row["Weapon"],
                    row["region_txt"],
                    row["country_txt"],
                    row["city"],
                    row["targtype1_txt"],
                    date,
                    row["Fatalities"],
                    row["Injuries"],
                    row["latitude"],
                    row["longitude"]
                )

        print("הנתונים הוזנו בהצלחה לגרף.")