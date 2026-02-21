# App Dockerfile for the FastAPI service
FROM python:3.10-slim

# Install curl and Java because ETL uses PySpark
RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app code
COPY app /app/app

# Expose API port
EXPOSE 8000

# Start uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
