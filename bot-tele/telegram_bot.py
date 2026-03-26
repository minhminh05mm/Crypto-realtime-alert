from telegram.ext import ApplicationBuilder, CommandHandler
from config import TOKEN
from handlers.commands import start, price, report

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("price", price))
app.add_handler(CommandHandler("report", report))

print("Bot is running...")
app.run_polling()