import requests
import json


def fetch_news():
    API_URL = 'http://eventregistry.org/api/v1/article/getArticles'

    params = {
        "action": "getArticles",
        "keyword": "terror attack",
        "ignoreSourceGroupUri": "paywall/paywalled_sources",
        "articlesPage": 1,
        "articlesCount": 100,
        "articlesSortBy": "socialScore",
        "articlesSortByAsc": "false",
        "dataType": ["news", "pr"],
        "forceMaxDataTimeWindow": 31,
        "resultType": "articles",
        "apiKey": "be7d1e47-d51a-46d5-8440-4b6b75304261"
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        data = response.json()

        # Let's first print the structure of the response to understand it
        print("Response structure:")
        print(json.dumps(data, indent=2))  # Print first 500 chars of formatted JSON

        # Now handle the data more carefully
        if isinstance(data, dict) and 'articles' in data:
            articles = data['articles']
            if isinstance(articles, list):
                for article in articles:
                    if isinstance(article, dict):
                        print(f"Title: {article.get('title', 'No title')}")
                        print(f"Source: {article.get('source', 'No source')}")
                        print(f"Date: {article.get('date', 'No date')}")
                    else:
                        print(f"Article data: {article}")
                    print("-" * 50)
        else:
            print("Unexpected response format:")
            print(json.dumps(data, indent=2))

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return None


if __name__ == "__main__":
    fetch_news()