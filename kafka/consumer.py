import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"

# event contract (see the README): every field must be present
CONTRACT_FIELDS = ["event_id", "country", "year", "gdp", "family", "health", "freedom",
                   "generosity", "corruption", "actual_happiness_score", "event_time"]
INDICATORS = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
NUMERIC_FIELDS = INDICATORS + ["actual_happiness_score"]

# the happiness score lives in [0, 10]: it is the valid range of the actual score
# and the safety net for the predicted score
SCORE_MIN = 0.0
SCORE_MAX = 10.0

STATUSES = ["VALID", "INVALID_SCHEMA", "INVALID_VALUES", "PREDICTION_ERROR"]


# ----------------------------------------------------------------- database
def get_connection():
    load_dotenv(BASE_DIR / ".env")
    try:
        return psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
    except KeyError as error:
        sys.exit(f"Missing variable {error} in the .env file.")
    except psycopg2.OperationalError as error:
        sys.exit(f"Cannot connect to PostgreSQL: {error}")


def store_raw(conn, message, payload, received_at):
    # Step 2: the original message is saved BEFORE any validation or prediction.
    # Returns (raw_event_id, already_processed). A Kafka record that was already stored
    # (same topic, partition and offset) is not stored twice.
    key = message.key().decode("utf-8", errors="replace") if message.key() else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_happiness_events
                (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload,
                 event_id, processing_status, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'RECEIVED', %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            RETURNING raw_event_id
            """,
            (message.topic(), message.partition(), message.offset(), key, payload,
             peek_event_id(payload), received_at),
        )
        row = cur.fetchone()
        if row is not None:
            conn.commit()
            return row[0], False
        cur.execute(
            """
            SELECT raw_event_id, processing_status FROM raw_happiness_events
            WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
            """,
            (message.topic(), message.partition(), message.offset()),
        )
        raw_event_id, status = cur.fetchone()
    conn.commit()
    # a row still in RECEIVED was left unfinished by a previous run: it is processed now
    return raw_event_id, status != "RECEIVED"


def set_status(conn, raw_event_id, status, detail):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw_happiness_events
            SET processing_status = %s, status_detail = %s, processed_at = %s
            WHERE raw_event_id = %s
            """,
            (status, detail, datetime.now(), raw_event_id),
        )
    conn.commit()


def store_prediction(conn, raw_event_id, event, predicted):
    # Step 7: dimension (if new) + fact row + final status, all in one transaction
    country = event["country"].strip()
    actual = float(event["actual_happiness_score"])
    with conn.cursor() as cur:
        cur.execute("INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING",
                    (country,))
        cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country,))
        country_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO fact_predictions
                (raw_event_id, country_id, date_id, actual_score, predicted_score,
                 prediction_error, event_time, prediction_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (raw_event_id, country_id, event["year"], actual, predicted, actual - predicted,
             datetime.fromisoformat(event["event_time"]), datetime.now()),
        )
        cur.execute(
            """
            UPDATE raw_happiness_events
            SET processing_status = 'VALID', status_detail = NULL, processed_at = %s
            WHERE raw_event_id = %s
            """,
            (datetime.now(), raw_event_id),
        )
    conn.commit()


# --------------------------------------------------------------- validation
def decode_payload(message):
    value = message.value()
    return value.decode("utf-8", errors="replace") if value is not None else ""


def peek_event_id(payload):
    # only reads the event_id (for traceability); the payload itself is stored untouched
    try:
        data = json.loads(payload)
    except ValueError:
        return None
    if isinstance(data, dict) and isinstance(data.get("event_id"), str):
        return data["event_id"]
    return None


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(payload, conn):
    # Step 3: validate against the event contract.
    # Returns (status, detail, event); event is None when the status is not VALID.
    try:
        event = json.loads(payload)
    except ValueError:
        return "INVALID_SCHEMA", "payload is not valid JSON", None
    if not isinstance(event, dict):
        return "INVALID_SCHEMA", "payload is not a JSON object", None

    # schema: required fields and data types
    for field in CONTRACT_FIELDS:
        if field not in event:
            return "INVALID_SCHEMA", f"missing field: {field}", None
    for field in ("event_id", "country", "event_time"):
        if not isinstance(event[field], str):
            return "INVALID_SCHEMA", f"{field} must be a string", None
    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return "INVALID_SCHEMA", "year must be an integer", None
    for field in NUMERIC_FIELDS:
        if not is_number(event[field]):
            return "INVALID_SCHEMA", f"{field} must be a number", None

    # values
    if event["event_id"].strip() == "":
        return "INVALID_VALUES", "event_id is empty", None
    if event["country"].strip() == "":
        return "INVALID_VALUES", "country is empty", None
    for field in NUMERIC_FIELDS:
        if not math.isfinite(event[field]):
            return "INVALID_VALUES", f"{field} is not a finite number", None
    for field in INDICATORS:
        if event[field] < 0:
            return "INVALID_VALUES", f"{field} must be >= 0: {event[field]}", None
    if not SCORE_MIN <= event["actual_happiness_score"] <= SCORE_MAX:
        return "INVALID_VALUES", f"actual_happiness_score must be between {SCORE_MIN:g} and {SCORE_MAX:g}", None
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return "INVALID_VALUES", "event_time is not a valid ISO-8601 timestamp", None

    # reference data and uniqueness
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dim_date WHERE date_id = %s", (event["year"],))
        if cur.fetchone() is None:
            return "INVALID_VALUES", f"year not in dim_date: {event['year']}", None
        cur.execute("SELECT 1 FROM raw_happiness_events WHERE event_id = %s AND processing_status = 'VALID'",
                    (event["event_id"],))
        if cur.fetchone() is not None:
            return "INVALID_VALUES", f"duplicate event_id: {event['event_id']}", None
    return "VALID", None, event


# --------------------------------------------------------------- prediction
def predict_score(model, features, event):
    # Steps 4-6: same feature order as in training; the Pipeline applies the same preprocessing
    row = pd.DataFrame([[float(event[name]) for name in features]], columns=features)
    predicted = float(model.predict(row)[0])
    if not SCORE_MIN <= predicted <= SCORE_MAX:   # also true for NaN
        raise ValueError(f"predicted score out of range [{SCORE_MIN:g}, {SCORE_MAX:g}]: {predicted:.1f}")
    return predicted


# --------------------------------------------------------------------- main
def handle_message(message, conn, model, features, stats):
    payload = decode_payload(message)
    raw_event_id, already_processed = store_raw(conn, message, payload, datetime.now())
    where = f"partition {message.partition()} offset {message.offset()}"

    if already_processed:
        print(f"SKIPPED  | {where} | already stored and processed")
        return

    status, detail, event = validate_event(payload, conn)
    if status == "VALID":
        try:
            predicted = predict_score(model, features, event)
            store_prediction(conn, raw_event_id, event, predicted)
        except Exception as error:
            conn.rollback()
            status, detail = "PREDICTION_ERROR", str(error)

    if status != "VALID":
        set_status(conn, raw_event_id, status, detail)
        stats["counts"][status] += 1
        label = event["event_id"] if event else (peek_event_id(payload) or "?")
        print(f"REJECTED | {label} | {status} | {detail} | {where}")
        return

    error_value = float(event["actual_happiness_score"]) - predicted
    stats["counts"]["VALID"] += 1
    stats["abs_error_sum"] += abs(error_value)
    print(f"PROCESSED | {event['event_id']} | {event['country']} {event['year']} | "
          f"actual {event['actual_happiness_score']:.3f} | predicted {predicted:.3f} | "
          f"error {error_value:+.3f} | {where}")
    if stats["counts"]["VALID"] % 25 == 0:
        print(f"--- {stats['counts']['VALID']} predictions so far | "
              f"running mean absolute error {stats['abs_error_sum'] / stats['counts']['VALID']:.4f}")


def main():
    if not MODEL_PATH.exists():
        sys.exit(f"Model not found: {MODEL_PATH}. Run notebooks/model_training.ipynb first.")
    model = joblib.load(MODEL_PATH)               # Step 5: load the serialized model once
    features = list(model.feature_names_in_)      # the features and order used in training
    unknown = [name for name in features if name not in CONTRACT_FIELDS]
    if unknown:
        sys.exit(f"The model needs features that are not in the event contract: {unknown}")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM raw_happiness_events LIMIT 1")
    except psycopg2.Error:
        conn.rollback()
        sys.exit("The tables do not exist yet. Run sql/create_tables.sql first.")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    stats = {"counts": {status: 0 for status in STATUSES}, "abs_error_sum": 0.0}
    print(f"Model features (in order): {features}")
    print(f"Consumer group '{GROUP_ID}' waiting for events on '{TOPIC}'... Press Ctrl + C to stop.\n")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue
            try:
                handle_message(message, conn, model, features, stats)
            except Exception as error:   # an unexpected problem must not stop the pipeline
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"UNEXPECTED ERROR | partition {message.partition()} offset {message.offset()} | {error}")
                if conn.closed:
                    print("Database connection lost. Stopping.")
                    break
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
        conn.close()
        print("Summary:", ", ".join(f"{status}={count}" for status, count in stats["counts"].items()))


if __name__ == "__main__":
    main()
