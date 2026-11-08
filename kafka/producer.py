import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import KafkaException, Producer

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_FILE = BASE_DIR / "data" / "streaming" / "happiness_stream.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"
EXPECTED_PARTITIONS = 3

# event field -> column of the CSV, in the order of the event contract
FIELD_MAP = {
    "country": "country",
    "year": "year",
    "gdp": "gdp",
    "family": "family",
    "health": "health",
    "freedom": "freedom",
    "generosity": "generosity",
    "corruption": "corruption",
    "actual_happiness_score": "score",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Send the records of a CSV file to Kafka, one event at a time.")
    parser.add_argument("--file", default=str(DEFAULT_FILE),
                        help="CSV file to send (default: the clean stream data/streaming/happiness_stream.csv)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="seconds to wait between events (default: 0.5)")
    parser.add_argument("--id-prefix", default="E",
                        help="prefix of the generated event ids: E001, E002... (default: E)")
    return parser.parse_args()


def convert_if_numeric(value):
    # clean numeric text becomes a number; anything else stays as text so the consumer can reject it
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def build_event(row, event_id):
    event = {"event_id": event_id}
    for event_field, column in FIELD_MAP.items():
        value = (row.get(column) or "").strip()
        if value == "":
            continue  # empty cell -> the key is omitted (missing field)
        event[event_field] = value if event_field == "country" else convert_if_numeric(value)
    event["event_time"] = datetime.now().isoformat(timespec="seconds")  # moment the producer emits the event
    return event


def check_topic(producer):
    # list_topics() without a topic name does not create anything.
    # Without this check, Kafka could auto-create the topic with a single partition.
    topics = producer.list_topics(timeout=10).topics
    if TOPIC not in topics:
        print(f"Topic '{TOPIC}' does not exist. Create it first with 3 partitions (see the README).")
        return False
    partitions = len(topics[TOPIC].partitions)
    if partitions != EXPECTED_PARTITIONS:
        print(f"Topic '{TOPIC}' has {partitions} partition(s); {EXPECTED_PARTITIONS} are required.")
        return False
    return True


def on_delivery(error, message):
    if error is not None:
        print(f"DELIVERY FAILED | {error}")


def main():
    args = parse_args()

    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = BASE_DIR / file_path
    if not file_path.exists():
        sys.exit(f"File not found: {file_path}")

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        topic_ok = check_topic(producer)
    except KafkaException as error:
        sys.exit(f"Cannot reach Kafka at {BOOTSTRAP_SERVERS}: {error}")
    if not topic_ok:
        sys.exit(1)

    with open(file_path, encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    print(f"Sending {len(rows)} events from {file_path.name} to '{TOPIC}' (delay {args.delay} s).")
    print("Press Ctrl + C to stop.\n")

    sent = 0
    try:
        for number, row in enumerate(rows, start=1):
            # an event_id written in the file is used as is; otherwise one is generated
            event_id = (row.get("event_id") or "").strip() or f"{args.id_prefix}{number:03d}"
            event = build_event(row, event_id)

            # key = country: the events of the same country always go to the same partition
            key = event.get("country") or "UNKNOWN_COUNTRY"

            producer.produce(topic=TOPIC, key=key, value=json.dumps(event), on_delivery=on_delivery)
            producer.poll(0)
            sent += 1
            print(f"Sent: {event_id} | key={key} | event={event}")

            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()

    print(f"Producer finished: {sent} of {len(rows)} events sent.")


if __name__ == "__main__":
    main()
