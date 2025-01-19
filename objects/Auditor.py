from kafka import KafkaConsumer
import json


class Auditor:
    def __init__(self, bootstrap_servers, topic, group_id="auditor_group"):
        """
        Auditor class to consume and log messages from a Kafka topic.

        :param bootstrap_servers: Kafka bootstrap servers (comma-separated).
        :param topic: Kafka topic to monitor.
        :param group_id: Unique consumer group ID for the auditor.
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=self._safe_deserializer,
        )

    @staticmethod
    def _safe_deserializer(value):
        """
        Deserializes Kafka messages. Attempts to decode as JSON, falls back to plain text if JSON fails.

        :param value: Raw message value.
        :return: Decoded message as a dictionary or plain text.
        """
        try:
            return json.loads(value.decode("utf-8"))
        except (json.JSONDecodeError, AttributeError):
            return {"raw_message": value.decode("utf-8")}

    def collect_logs(self):
        """
        Continuously consume messages from the topic and log user-related metadata.
        """
        print("Auditor is listening for messages...")
        try:
            for message in self.consumer:
                metadata = {
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key.decode("utf-8") if message.key else None,
                    "timestamp": message.timestamp,
                }
                value = message.value
                log_entry = {"metadata": metadata, "content": value}
                self.log_event(log_entry)
        except KeyboardInterrupt:
            print("Auditor stopped by user.")
        finally:
            self.close()

    def close(self):
        """
        Gracefully close the Kafka consumer connection.
        """
        print("Closing Auditor connection...")
        self.consumer.close()

    @staticmethod
    def log_event(event):
        """
        Log the event details (extend this to log to a file, database, etc.).

        :param event: Event dictionary containing message and metadata.
        """
        print(f"Audited Event: {json.dumps(event, indent=2)}")
