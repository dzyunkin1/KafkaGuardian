from typing import List
from objects.User import User
import threading
import time


def run_simulation(users: List[User], duration: float):
    # Start each user in its own thread
    threads = []
    for user in users:
        thread = threading.Thread(target=user.generate_messages)
        threads.append(thread)
        thread.start()

    # Run for the specified duration
    time.sleep(duration)

    # Stop all users
    for user in users:
        user.stop_generate_message()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Close all producers
    for user in users:
        user.close_producer()
