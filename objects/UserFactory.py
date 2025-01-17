from User import User
from typing import List


class UserFactory:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def create_user(self, mean, std) -> User:
        return User(self.producer, self.topic, mean, std)

    def create_multiple_users(self, configs) -> List[User]:
        return [self.create_user(mean, std) for mean, std in configs]
