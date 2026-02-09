FROM apache/spark:3.5.0

USER root

# Install only what Spark image does NOT already include
RUN pip install --no-cache-dir psycopg2-binary

# Create checkpoint directory with correct permissions
RUN mkdir -p /tmp/spark-checkpoints && chmod -R 777 /tmp/spark-checkpoints

# Working directory for spark-submit
WORKDIR /opt/spark/work-dir
