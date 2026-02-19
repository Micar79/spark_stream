FROM python:3.10-slim


# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0 delta-spark pyyaml

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Run the application
CMD ["python", "app/main.py"]
