# Use a base Python image
FROM python:3.10-slim

# Install Java, a prerequisite for Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_HOME="/opt/spark"
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYSPARK_PYTHON=python3

# Download and unpack Spark
WORKDIR $SPARK_HOME
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz -o spark.tgz && \
    tar xzf spark.tgz --strip-components 1 && \
    rm spark.tgz

# Install the PySpark library via pip
RUN pip install pyspark==3.4.1

# Set the working directory for your application
WORKDIR /app

# Optional: Copy your application code into the container
# COPY . .

# Run the application
CMD ["python", "app/main.py"]
