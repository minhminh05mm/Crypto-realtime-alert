from datetime import datetime
from services.market import get_market_data
from services.news import get_crypto_news

def build_market_report():
    btc, eth = get_market_data()
    news = get_crypto_news()

    today = datetime.now().strftime("%d/%m/%Y")

    trend = "Bullish " if float(btc['priceChangePercent']) > 0 else "Bearish "

    return f"""
<b>📊 TỔNG HỢP THỊ TRƯỜNG</b>
<i>Ngày {today}</i>

<b>🪙 TIỀN ĐIỆN TỬ</b>

• <b>BTC</b>: ${float(btc['lastPrice']):,.2f} 
  (<b>{float(btc['priceChangePercent']):.2f}%</b> 24h)

• <b>ETH</b>: ${float(eth['lastPrice']):,.2f} 
  (<b>{float(eth['priceChangePercent']):.2f}%</b> 24h)

<b>📈 Xu hướng:</b> {trend}

<b>📰 Tin tức nổi bật:</b>
{news}

-------------------------
🤖 Crypto Alert Bot
"""