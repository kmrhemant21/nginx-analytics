import os
import requests
from kafka import KafkaProducer
import json
import time
import logging
from typing import Generator, Optional, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
LOKI_URL = os.getenv("LOKI_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
LOKI_USERNAME = os.getenv("LOKI_USERNAME")
LOKI_PASSWORD = os.getenv("LOKI_PASSWORD")

def create_kafka_producer(retries: int = 20, backoff_sec: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 6),
                retries=5,
                retry_backoff_ms=1000,
                request_timeout_ms=15000,
                max_block_ms=30000
            )
            producer.partitions_for(KAFKA_TOPIC)
            logger.info("Kafka connection established.")
            return producer
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt}/{retries} failed: {e}")
            time.sleep(backoff_sec)
    raise ConnectionError(f"Could not connect to Kafka after {retries} attempts")


def fetch_loki_logs(namespace: str, limit: int = 1000) -> Generator[str, None, None]:
    params = {
        "query": f'{{namespace="{namespace}"}}',
        "limit": limit,
        "direction": "FORWARD"
    }
    auth = None
    if LOKI_USERNAME and LOKI_PASSWORD:
        auth = (LOKI_USERNAME, LOKI_PASSWORD)
    try:
        response = requests.get(
            f"{LOKI_URL}/loki/api/v1/query_range",
            params=params,
            timeout=10,
            auth=auth   # <-- Add this line
        )
        response.raise_for_status()
        data = response.json()
        streams = data.get("data", {}).get("result", [])

        for stream in streams:
            for timestamp, log in stream.get("values", []):
                yield log
    except requests.RequestException as e:
        logger.error(f"Request to Loki failed: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode Loki response JSON: {e}")



# Validate and Send Logs to Kafka
def process_and_send_log(producer: KafkaProducer, log: str, topic: str) -> None:
    try:
        log_data: Optional[Dict] = json.loads(log)
        required_keys = {"request", "remote_addr", "status", "request_time"}

        if isinstance(log_data, dict) and required_keys.issubset(log_data.keys()):
            if log_data.get("server_name") == "dev.az.example.com":
                producer.send(topic, log_data)
                logger.info(f"Sent log to topic '{topic}'")
            else:
                logger.info(f"Log skipped due to server name: {log_data.get('server_name')}")
        else:
            logger.warning(f"Log entry missing required keys: {log_data}")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding log JSON: {e}")


# Main Processing Loop
def run_log_processor():
    producer = create_kafka_producer()
    logger.info("NGINX log processor started.")

    try:
        while True:
            start_time = time.time()
            log_count = 0

            for log in fetch_loki_logs(namespace="ingress-nginx"):
                process_and_send_log(producer, log, KAFKA_TOPIC)
                log_count += 1

                if log_count % 100 == 0:
                    logger.info(f"Processed {log_count} logs.")

            producer.flush()
            elapsed = time.time() - start_time
            logger.info(f"Completed batch: {log_count} logs in {elapsed:.2f}s")

            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        producer.close(timeout=10)
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    run_log_processor()
