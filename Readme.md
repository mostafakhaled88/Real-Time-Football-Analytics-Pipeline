Real-Time Football Analytics Pipeline ⚽📊 A high-performance streaming
data pipeline that ingests live football match events, processes them
using PySpark Structured Streaming, and stores them in PostgreSQL for
real-time analytics.

🏗️ Architecture Data Source: Custom Python Producer simulating live
match events (Passes, Shots, Goals, etc.).

Message Broker: Apache Kafka for high-throughput, fault-tolerant message
queuing.

Stream Processing: PySpark (Spark 3.5.0) performing schema enforcement,
data transformation, and micro-batching.

Storage: PostgreSQL as the relational sink for structured event storage.

Orchestration: Docker & Docker Compose for local environment
consistency.

🚀 Getting Started 1. Prerequisites Docker & Docker Desktop
(Windows/Linux/Mac)

Python 3.9+ (for the Producer)

pip install kafka-python

2\. Launch the Infrastructure Clone the repo and run:

Bash

docker-compose up -d This starts Kafka, Zookeeper, Spark Master/Worker,
and PostgreSQL.

3\. Initialize the Database Connect to Postgres (localhost:5433) and
run:

SQL

CREATE TABLE football_events ( event_id VARCHAR(50) PRIMARY KEY,
timestamp TIMESTAMP, team VARCHAR(50), player VARCHAR(100), event_type
VARCHAR(20), details TEXT, loc_x DOUBLE PRECISION, loc_y DOUBLE
PRECISION, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ); 4. Start
the Stream The Spark job starts automatically via the spark-submit
container. Monitor it using:

Bash

docker logs -f spark-submit 5. Run the Live Producer In a separate
terminal, start the match simulation:

Bash

python producer.py 🛠️ Technical Decisions & Challenges Idempotency &
Fault Tolerance Checkpointing: Enabled Spark checkpointing to allow the
stream to recover from failures without losing data.

Schema Enforcement: Defined a strict StructType to ensure data quality
and handle nested JSON objects from Kafka.

Docker Networking Implemented a bridge network (football-net) to allow
Spark to resolve Kafka and Postgres containers by service name.

Handled UnknownHostException by binding Spark Master to an explicit host
and utilizing a 20-second startup delay.

Performance Optimization Micro-batching: Set a 5-second trigger to
balance between real-time latency and database write overhead.

Resource Management: Used .persist() and .unpersist() within the
foreachBatch function to prevent redundant re-computations of streaming
data.

📈 Sample Analytics Once the pipeline is running, you can run queries
like this in Postgres:

Live Scoreboard:

SQL

SELECT team, COUNT(\*) as goals FROM football_events WHERE event_type =
\'GOAL\' GROUP BY team; 📁 Project Structure Plaintext

. ├── docker-compose.yml \# Infrastructure orchestration ├── producer.py
\# Python Kafka producer └── spark/ └── spar
