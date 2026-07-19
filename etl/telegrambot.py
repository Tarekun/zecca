import random
import telebot
from dotenv import load_dotenv
import os
from typing import Literal, Optional

from etl.logger import get_logger

logger = get_logger(__name__)

# extensions telebot can send with a native preview; anything else falls
# back to send_document
ANIMATION_EXTENSIONS = {".gif"}
PHOTO_EXTENSIONS = {".jpeg", ".jpg", ".png", ".webp"}


def _send_random_media(dir_path: str, messages: dict[str, str], text_suffix: str = ""):
    filename = random.choice(list(messages.keys()))
    text = messages[filename] + text_suffix
    media_path = os.path.join(dir_path, filename)
    send_message_to_group(text, media_path)


def send_success_media():
    dir_path = f"./media/gifs-success"
    messages = {
        "robin gan bands.jpeg": "Another day another dora",
        "robin gan calc.gif": "We making it out of the rice fields with this one 🔥",
    }
    _send_random_media(dir_path, messages)


def send_error_media(error_message: str = ""):
    dir_path = f"./media/gifs-error"
    messages = {
        "crying chinese unc.jpeg": "Somesing wentz wong",
        "asian-business-woman.webp": "Deverope, you are a disgrace to tha famiry",
    }
    text_suffix = f"\n{error_message}" if error_message else ""
    _send_random_media(dir_path, messages, text_suffix)


def send_message_to_group(text: str, media_path: Optional[str] = None):
    try:
        load_dotenv()

        BOT_TOKEN = os.getenv("BOT_TOKEN")
        if BOT_TOKEN is None:
            raise ValueError("Error: BOT_TOKEN env variable does not exist")
        bot = telebot.TeleBot(BOT_TOKEN)

        GROUP_CHAT_ID = os.getenv("CHAT_ID")
        if GROUP_CHAT_ID is None:
            raise ValueError("Error: CHAT_ID env variable does not exist")

        if media_path is None:
            bot.send_message(GROUP_CHAT_ID, text)
        else:
            extension = os.path.splitext(media_path)[1].lower()
            with open(media_path, "rb") as media_file:
                if extension in ANIMATION_EXTENSIONS:
                    bot.send_animation(GROUP_CHAT_ID, media_file, caption=text)
                elif extension in PHOTO_EXTENSIONS:
                    bot.send_photo(GROUP_CHAT_ID, media_file, caption=text)
                else:
                    bot.send_document(GROUP_CHAT_ID, media_file, caption=text)
        logger.info("Telegram message sent")
    except Exception as e:
        logger.error("Could not send Telegram message: %s", e)
