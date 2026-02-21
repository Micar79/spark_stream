# DE Web App — End-to-end Data Engineering Project

This scaffold implements a FastAPI web application that demonstrates common data engineering concepts:

- Data ingestion (CSV upload + Kafka producer)
- Storage (Postgres for metadata, file storage for raw/processed data)
- Batch ETL using PySpark (placeholder)
- Streaming integration via Kafka (producer example)
- Docker + docker-compose to run services (Postgres, Zookeeper, Kafka, App)
- GitHub Actions CI (tests)

Quick start

1. Build and run services:

```bash
cd de_web_app
docker-compose up --build
```

2. Open app at `http://localhost:8000`

3. Upload a CSV to `/upload` (multipart form) to send to Kafka / store raw file.
4. Trigger a Spark ETL via `/trigger-etl` (calls a placeholder Spark job in the container).

This project is a scaffold — extend `app/etl.py` with real Spark jobs and add more tests.
