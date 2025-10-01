import requests
import json
import time
import os

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

class whatsappConnection:
    
    def __init__(self, **kwargs):

        self.defines_parameters()


    def defines_parameters(self):

        self.whatsapp_token = 'EAALVecfqwoEBO00o0nSLY3xZB6sqr67ghH0NbOa7EZBC5JUODhcdfKoccK8mILasXKmdczWOZAduZC2cq7P9wy4pC8Yhrliw5IZC7F35bw7pXZCZAoqXhW6wULZBIyI5uhFTI7VzxZCWsIovZA93nq1WW1ec2sCYha1aUSaR7nkZBw1gwLKlCsYezqsZCs1Xb1Njeh8HUQZDZD'
        self.phone_number_id = '295435883644967' #Test number

        self.url = f'https://graph.facebook.com/v19.0/{self.phone_number_id}/messages'

        self.headers = {
            'Authorization': f'Bearer {self.whatsapp_token}',
            'Content-Type': 'application/json'
        }

        self.headersimg = {
            'Authorization': f'Bearer {self.whatsapp_token}',
        }

    def send_message(self, recepient_phone_number, msg_body, **kwargs):

        option = kwargs.get('option',None)

        if msg_body:
            msg_body = msg_body
        else:
            raise Exception('No given message to deliver to whatsapp!')

        payload = {
            'messaging_product': 'whatsapp',
            'recipient_type': 'individual',
            'to': recepient_phone_number,
            'type': 'text',
            'text': {
                'body': msg_body
            }
        }

        if option == 'send template':

            payload = {
            "messaging_product": "whatsapp",
            "to": recepient_phone_number,
            "type": "template",
            "template": {
                "name": "iris_de_superfrutti",
                "language": {"code": "es_MX"}
            },
            "components": {
                "type": "body",
                "parameters": {"type": "text", "text": "oi"}
            }
            }

        response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
        print(response.text)

    
    def send_image(self, recepient_phone_number, image_buffer, **kwargs):

        msg_body = kwargs.get('msg_body','')

        media_url = f'https://graph.facebook.com/v23.0/{self.phone_number_id}/media'

        image_buffer.name = 'img.png'
        image_buffer.seek(0)
        files = { "file" : ('whatsapp', image_buffer, 'image/png')}
        #files = { "file" : ('whatsapp', '@/Strategy_summary.png', 'image/png')}

        payload = {
            'messaging_product':'whatsapp',
            'type':'image/png'
            }

        response = requests.post(media_url, headers=self.headersimg, params=payload, files=files)
        res = response.json()
        print(res)

        media_id = res['id']

        payload = {
            'messaging_product': 'whatsapp',
            'recipient_type': 'individual',
            'to': recepient_phone_number,
            'type': 'image',
            'image': {
                'id': str(media_id),
                'caption': msg_body
            }
        }

        response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
        print(response.json())

    def send_file(self, recepient_phone_number, document_buffer, extension):

        media_url = f'https://graph.facebook.com/v23.0/{self.phone_number_id}/media'

        mimetypes_dict = {
            'xlsx':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }

        mimetype = mimetypes_dict[extension]

        document_buffer.name = 'document.' + extension
        document_buffer.seek(0)

        files = { "file" : ('whatsapp', document_buffer, mimetype)}
        #files = { "file" : ('whatsapp', '@/Strategy_summary.png', 'image/png')}

        payload = {
            'messaging_product':'whatsapp',
            'type': mimetype
            }

        response = requests.post(media_url, headers=self.headersimg, params=payload, files=files)
        res = response.json()
        print(res)

        media_id = res['id']

        payload = {
            'messaging_product': 'whatsapp',
            'recipient_type': 'individual',
            'to': recepient_phone_number,
            'type': 'document',
            'document': {
                'id': str(media_id)
            }
        }

        response = requests.post(self.url, headers=self.headers, data=json.dumps(payload))
        print(response.json())






