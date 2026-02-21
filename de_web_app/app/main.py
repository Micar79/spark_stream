"""FastAPI app exposing ingestion and control endpoints."""
from fastapi import FastAPI, UploadFile, File, BackgroundTasks
import os
from app.ingest import handle_upload
from app.etl import run_spark_etl

app = FastAPI(title="DE Web App")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/upload")
async def upload(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """Upload CSV file and process in background: save to raw folder and produce to Kafka."""
    raw_dir = "/data/raw"
    os.makedirs(raw_dir, exist_ok=True)
    file_path = os.path.join(raw_dir, file.filename)

    contents = await file.read()
    with open(file_path, "wb") as f:
        f.write(contents)

    # Launch background processing: store metadata, and produce to Kafka
    if background_tasks is not None:
        background_tasks.add_task(handle_upload, file_path)
    else:
        handle_upload(file_path)

    return {"uploaded": file.filename}

@app.post("/trigger-etl")
def trigger_etl():
    """Trigger the Spark ETL job (synchronous placeholder)."""
    # In production you'd schedule or submit a real Spark job
    run_spark_etl()
    return {"status": "etl_started"}

@app.get("/metrics")
def metrics():
    """Return simple metrics (placeholder)."""
    return {"ingested_files": len(os.listdir("/data/raw")) if os.path.exists("/data/raw") else 0}
