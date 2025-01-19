from kafka import KafkaProducer
from objects.UserFactory import UserFactory
from objects.User import User
from objects.Auditor import Auditor
from utils.simulate_users import run_simulation
from typing import List


def main():
    bootstrap_servers = "localhost:9092"
    topic = "demo-messages"

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    auditor = Auditor(bootstrap_servers=bootstrap_servers, topic=topic)

    factory: UserFactory = UserFactory(producer, topic)
    user_configs: List[tuple] = [
        (1.0, 0.5),  # User 1
        (1.5, 0.7),  # User 2
    ]
    users: List[User] = factory.create_multiple_users(user_configs)
    run_simulation(users, duration=5)

    auditor.collect_logs()

    # Run simulation for 5 seconds


if __name__ == "__main__":
    main()
