import telebot


BOT_TOKEN = ""


bot = telebot.TeleBot(BOT_TOKEN)

GROUP_CHAT_ID = 0

def send_message_to_group(text: str):
    """
    Invia un messaggio al gruppo specificato.
    :param text: Il testo da inviare
    """
    try:
        bot.send_message(GROUP_CHAT_ID, text)
        print("Messaggio inviato con successo!")
    except Exception as e:
        print(f"Errore durante l'invio del messaggio: {e}")

# Esempio di utilizzo
if __name__ == "__main__":
    send_message_to_group("⚠⚠⚠ WARING ⚠⚠⚠")
