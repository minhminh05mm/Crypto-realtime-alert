import requests
from config import NEWS_API_KEY

def get_crypto_news():
    url = f"https://newsapi.org/v2/everything?q=bitcoin OR ethereum OR BTC OR ETH&language=vi&sortBy=publishedAt&apiKey={NEWS_API_KEY}&pageSize=5"
    
    data = requests.get(url).json()

    news_text = ""

    for article in data.get("articles", [])[:3]:
        title = article["title"]
        link = article["url"]
        news_text += f"• <a href='{link}'>{title}</a>\n"

    return news_text