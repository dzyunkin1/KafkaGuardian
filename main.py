from kafka import KafkaProducer
from objects import User, UserFactory
from utils.simulate_users import run_simulation
from typing import List


def main():
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    topic = "demo-messages"

    factory = UserFactory(producer, topic)
    user_configs = [
        (1.0, 0.5),  # User 1
        (1.5, 0.7),  # User 2
    ]
    users = factory.create_multiple_users(user_configs)

    # Run simulation for 5 seconds
    run_simulation(users, duration=5)


# Example usage:
if __name__ == "__main__":
    main()
