import threading
import time
from numpy.random import normal
from kafka import KafkaProducer
from numpy.random import random


class User:
    def __init__(self, producer: KafkaProducer, topic: str, mean: float, std: float):
        self.producer = producer
        self.topic = topic
        self.mean = mean
        self.std = std
        self._stop_event = threading.Event()
        self.message_count = 0
        self.username = random(size=1) * 100

    def generate_messages(self) -> None:
        self._stop_event.clear()
        while not self._stop_event.is_set():
            message = f"User {self.username[0]} writing message {self.message_count}"
            self.message_count += 1

            try:
                self.producer.send(self.topic, message.encode("utf-8"))
                sleep_time = normal(self.mean, self.std, size=1)
                time.sleep(abs(sleep_time[0]))
            except Exception as e:
                print(f"Error sending message: {e}")

    def stop_generate_message(self) -> None:
        self._stop_event.set()
        self.producer.flush()

    def close_producer(self) -> None:
        self.producer.close()
