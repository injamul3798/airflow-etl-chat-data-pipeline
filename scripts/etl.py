import json
import os
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import execute_batch


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "chat_data.json"


def extract(file_path: Path = DATA_FILE) -> list[dict[str, Any]]:
    with file_path.open("r", encoding="utf-8-sig") as file:
        return json.load(file)


def transform(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    transformed: list[dict[str, Any]] = []

    for record in records:
        enriched = record.copy()
        enriched["message_length"] = len(enriched["message"])
        transformed.append(enriched)

    return transformed


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def load(records: list[dict[str, Any]]) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS messages (
        message_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        message TEXT NOT NULL,
        message_timestamp TIMESTAMP NOT NULL,
        response_time FLOAT NOT NULL,
        message_length INT NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    insert_sql = """
    INSERT INTO messages (
        user_id,
        message,
        message_timestamp,
        response_time,
        message_length
    )
    VALUES (%s, %s, %s, %s, %s);
    """

    payload = [
        (
            record["user_id"],
            record["message"],
            record["timestamp"],
            record["response_time"],
            record["message_length"],
        )
        for record in records
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            execute_batch(cur, insert_sql, payload)


def run_etl() -> None:
    records = extract()
    transformed_records = transform(records)
    load(transformed_records)


if __name__ == "__main__":
    run_etl()
