import requests
import json
from pymongo import MongoClient

# קביעת מפתח API וקישור
API_KEY = 'your_api_key_here'
API_URL = 'https://newsapi.ai/documentation?tab=searchArticles'

# שליפת מידע מה-API
def fetch_news():
    params = {
        "action": "getArticles",
        "keyword": "terror attack",
        "articlesCount": 10,  # הגבלת מספר התוצאות
        "articlesSortBy": "socialScore",
        "apiKey": API_KEY
    }
    response = requests.post(API_URL, json=params)
    if response.status_code == 200:
        print(response.json().g.get('articles', []))
        return response.json().get('articles', [])
    else:
        print("Error fetching data:", response.status_code)
        return []

# חיבור ל-MongoDB
# def connect_to_db():
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client['terror_analysis']
#     return db['real_time_news']
#
# # הוספת נתונים לבסיס הנתונים
# def store_in_db(articles, collection):
#     for article in articles:
#         collection.update_one(
#             {"uri": article["uri"]},  # זיהוי ייחודי למניעת כפילויות
#             {"$set": article},
#             upsert=True
#         )

# הרצת התהליך
if __name__ == "__main__":
    news_articles = fetch_news()
    if news_articles:
        collection = connect_to_db()
        store_in_db(news_articles, collection)
        print("Data successfully fetched and stored.")
    else:
        print("No articles fetched.")
