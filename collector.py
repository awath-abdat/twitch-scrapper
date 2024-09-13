import os
import time
import requests
import websocket
from dotenv import load_dotenv
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", None)
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", None)

MAX_JOIN_ATTEMPTS = int(os.getenv("MAX_JOIN_ATTEMPTS", 15))
TWITCH_CLIENT_USER_ID = os.getenv("TWITCH_CLIENT_USER_ID", "1133439087")
TWITCH_WEBSOCKET_HOST = "wss://eventsub.wss.twitch.tv/ws"


# Websocket functions
def on_open(_):
    print("Websocket has been opened.........")


def queue_message(_, message):
    broadcaster_user_id = message.get("event", {}).get("broadcaster_user_id", None)
    message_text = message.get("event", {}).get("message", {}).get("text", None)
    event_data = message.get("event", {})

    if not broadcaster_user_id or not message_text:
        return

    producer.send(
        "twitch-chat",
        value={
            "broadcaster_user_id": broadcaster_user_id,
            "message": message_text,
            "event_data": event_data,
        },
    )


def on_error(_, error):
    print(f"Websocker error: {error}")


def on_close(_, status_code, close_message):
    print(f"Websocker has been closed with status code {status_code}: {close_message}")


class TwitchAPICollector:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.twitch.tv/helix/"
        self.oauth_token = None
        self.oauth_token_expires_in = 0

        oauth_token_response = requests.post(
            f"https://id.twitch.tv/oauth2/token?client_id={TWITCH_CLIENT_ID}&client_secret={TWITCH_CLIENT_SECRET}&grant_type=client_credentials"
        )

        if oauth_token_response.status_code == 200:
            data = oauth_token_response.json()
            self.oauth_token = data["access_token"]
            self.oauth_token_expires_in = data["expires_in"]

        print(oauth_token_response.json())
        self.headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.oauth_token}",
            "Content-Type": "application/json",
        }

    def get_live_streams(self):
        return requests.get(f"{self.base_url}streams", headers=self.headers).json()

    def subscribe_to_chat_stream(self, broadcaster_user_id):
        response = requests.post(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers=self.headers,
            data={
                "type": "channel.chat.message",
                "version": 1,
                "condition": {
                    "broadcaster_user_id": f"{broadcaster_user_id}",
                    "user_id": f"{TWITCH_CLIENT_USER_ID}",
                },
                "transport": {
                    "method": "websocket",
                    "session_id": "f8e0e0c584004f9891e5043122ee5854",
                },
            },
        )
        return response.json()

    def run(self):
        streams = self.get_live_streams()
        for index, stream in enumerate(streams["data"]):
            channel_id = stream["user_id"]
            self.subscribe_to_chat_stream(channel_id)

            # Sleep when limit has reached
            if index >= MAX_JOIN_ATTEMPTS:
                time.sleep(10)

        ws = websocket.WebsocketApp(
            TWITCH_WEBSOCKET_HOST,
            on_message=queue_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.on_open = on_open
        ws.run_forever()


if __name__ == "main":
    twitch_api_collector = TwitchAPICollector(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    twitch_api_collector.run()
