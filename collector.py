import time
import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")


class TwitchAPICollector:
    def __init__(self, client_id, oauth_token):
        self.client_id = client_id
        self.oauth_token = oauth_token
        self.base_url = "https://api.twitch.tv/helix/"
        self.headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.oauth_token}",
        }

    def get_live_streams(self):
        url = self.base_url + "streams"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_chat_messages(self, channel_id):
        # Example function to get chat messages
        url = f"https://api.twitch.tv/helix/chat/messages?broadcaster_id={channel_id}"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def collect_data(self):
        while True:
            streams = self.get_live_streams()
            for stream in streams["data"]:
                channel_id = stream["user_id"]
                chat_messages = self.get_chat_messages(channel_id)
                # Send chat messages to the queue
                self.send_to_queue(chat_messages)
            time.sleep(10)  # Polling interval

    def send_to_queue(self, messages):
        for message in messages:
            producer.send("twitch-chat", value=message.encode("utf-8"))
