import requests
import json
import time
import os

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


class telegramConnection:
    
    def __init__(self, **kwargs):

        """
        Add new bot on instagram account (search for the account BotFather)
        Assume the bot name is my_bot.
        Get the bot token by follow the instructions given in the chat after creation
        Create a telegram group

        1- Add the bot to the group.
        Go to the group, click on group name, click on Add members, in the searchbox search for your bot like this: @my_bot, select your bot and click add.

        2- Send a dummy message to the bot.
        You can use this example: /my_id @my_bot
        (I tried a few messages, not all the messages work. The example above works fine. Maybe the message should start with /)

        3- Go to following url: https://api.telegram.org/botXXX:YYYY/getUpdates
        replace XXX:YYYY with your bot token

        4- Look for "chat":{"id":-zzzzzzzzzz,
        -zzzzzzzzzz is your chat id (with the negative sign).

        5- Testing: You can test sending a message to the group with a curl:
        """

        self.defines_telegram_parameters()

    def defines_telegram_parameters(self):

        self.TELEGRAM_BOT_TOKEN = '8121151732:AAH-gUtnwB-upxfTao4oTkYJRyqmgvA1UCQ' #WilliamUyasan_bot
        self.CHAT_ID = -4948600790 #WU -> Stock positions group

    def get_group_chat_id(self):

        telegram_bot_url_status = f'https://api.telegram.org/bot{self.TELEGRAM_BOT_TOKEN}/getUpdates'
        #print(telegram_bot_url_status)

        response = requests.get(telegram_bot_url_status)
        res = response.json()

        chat_id = res['result'][0]['message']['chat']['id']

        self.CHAT_ID = chat_id
        
        return chat_id
    
    def send_message(self, message_text):

        url = f"https://api.telegram.org/bot{self.TELEGRAM_BOT_TOKEN}/sendMessage"

        payload = {
            'chat_id': self.CHAT_ID, 
            'text': message_text, 
            'parse_mode': 'html'
            }

        response = requests.post(url, data=payload).json()
        logger.info(response)

    def send_image(self, imagebuffer, message_text):

        url = f"https://api.telegram.org/bot{self.TELEGRAM_BOT_TOKEN}/sendPhoto"

        payload = {
            'chat_id': self.CHAT_ID,
            'caption': message_text,
            'parse_mode': 'html'
        }

        imagebuffer.name = 'img.png'
        imagebuffer.seek(0)
        files = {'photo': imagebuffer.getvalue()}


        response = requests.post(url=url, data=payload, files=files)
        res = response.json()
        logger.info(res)

