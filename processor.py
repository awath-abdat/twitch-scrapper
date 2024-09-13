import os
import boto3
from pymongo import MongoClient
from kafka import KafkaConsumer

consumer = KafkaConsumer("twitch-chat", bootstrap_servers="localhost:9092")
client = MongoClient("mongodb://localhost:27017/")
db = client["twitch_data"]
collection = db["chat_messages"]


def process_messages():
    for message in consumer:
        # insert message to the database
        collection.insert_one(message)

        broadcaster_user_id = message.get("broadcaster_user_id", None)
        message_count = (
            collection.count_documents({"broadcaster_user_id": broadcaster_user_id})
            if broadcaster_user_id
            else 0
        )

        # check message threshold
        if message_count and message_count > int(
            os.getenv("CHAT_MESSAGE_THRESHOLD", 1000)
        ):
            send_alert(message_count)
        message_count = 0


def send_alert(count):
    sns = boto3.client("sns")

    sns.publish(
        TopicArn="TwitchDataCollection",
        Message=f"Alert: High chat volume detected! Messages in the last minute: {count}",
        Subject="Twitch Chat Alert",
    )


if __name__ == "main":
    while True:
        process_messages()
