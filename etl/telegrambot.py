import telebot
from dotenv import load_dotenv
import os
from etl.logger import get_logger

logger = get_logger(__name__)


def send_message_to_group(text: str):
    try:
        load_dotenv()

        BOT_TOKEN = os.getenv("BOT_TOKEN")
        if BOT_TOKEN is None:
            raise ValueError("Error: BOT_TOKEN env variable does not exist")
        bot = telebot.TeleBot(BOT_TOKEN)

        GROUP_CHAT_ID = os.getenv("CHAT_ID")
        if GROUP_CHAT_ID is None:
            raise ValueError("Error: CHAT_ID env variable does not exist")

        bot.send_message(GROUP_CHAT_ID, text)
        logger.info("Telegram message sent")
    except Exception as e:
        logger.error("Could not send Telegram message: %s", e)
