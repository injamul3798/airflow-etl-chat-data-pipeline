# AI Data Pipeline

An end-to-end data engineering project that ingests chat data, transforms it, loads it into PostgreSQL, and orchestrates the workflow with Apache Airflow.
<img width="1365" height="660" alt="image" src="https://github.com/user-attachments/assets/e4ca979a-d84f-4c21-856a-0d7153b6b494" />

## Stack

- Python
- PostgreSQL
- Apache Airflow
- Docker Compose

## Project Structure

```text
.
├── dags/
│   └── pipeline_dag.py
├── data/
│   └── chat_data.json
├── scripts/
│   └── etl.py
├── docker-compose.yml
├── main.py
└── requirements.txt
```

## How It Works

- `extract()`: reads chat data from JSON
- `transform()`: adds `message_length`
- `load()`: creates the `messages` table and inserts records
- Airflow runs the ETL through the `chat_pipeline` DAG

## Run The Project

```bash
docker compose up airflow-init
docker compose up
```

Open `http://localhost:8080`

Login:

- Username: `admin`
- Password: `admin`

Then trigger `chat_pipeline`.

## Run Locally

```bash
pip install -r requirements.txt
python main.py
```

## Purpose

This project shows the core flow of a real data pipeline:

- source data
- ETL processing
- database loading
- workflow orchestration
