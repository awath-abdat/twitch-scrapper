import os
import time
import boto3
from pymongo import MongoClient
from kafka import KafkaConsumer

consumer = KafkaConsumer("twitch-chat", bootstrap_servers="localhost:9092")
client = MongoClient("mongodb://localhost:27017/")
db = client["twitch_data"]
collection = db["chat_messages"]


def process_messages():
    message_count = 0
    time_window_start = time.time()

    for message in consumer:
        message_count += 1
        if time.time() - time_window_start >= 60:  # 1 minute window

            # insert message to the database
            collection.insert_one(message)

            # check message threshold
            if message_count > int(os.getenv("CHAT_MESSAGE_THRESHOLD", 1000)):
                send_alert(message_count)
            message_count = 0
            time_window_start = time.time()


def send_alert(count):
    sns = boto3.client("sns")

    sns.publish(
        TopicArn="TwitchDataCollection",
        Message=f"Alert: High chat volume detected! Messages in the last minute: {count}",
        Subject="Twitch Chat Alert",
    )

    print(f"Alert: High chat volume detected! Messages in the last minute: {count}")
