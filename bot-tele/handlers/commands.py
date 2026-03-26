from telegram import Update
from telegram.ext import ContextTypes
import requests
from services.report import build_market_report

alerts = []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 Crypto Bot Ready!")

async def price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /price btc")
        return

    coin = context.args[0].upper() + "USDT"
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={coin}"
    data = requests.get(url).json()

    await update.message.reply_text(f"{coin}: {data['price']}")

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = build_market_report()

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )