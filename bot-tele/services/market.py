import requests

def get_market_data():
    btc = requests.get("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT").json()
    eth = requests.get("https://api.binance.com/api/v3/ticker/24hr?symbol=ETHUSDT").json()
    return btc, eth