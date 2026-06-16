# 🚀 Real-Time Stock Market Data Pipeline using Kafka & AWS

## 📊 Overview
This project is an end-to-end real-time data engineering pipeline that streams stock market data using Apache Kafka, processes it in real time, and stores it in AWS S3 for further analysis using AWS Glue and Athena.

The system simulates live stock market data ingestion and builds a scalable streaming architecture for financial data analytics.

---

## 🏗️ Architecture

Data Source (CSV) → Python Producer → Apache Kafka → Python Consumer → AWS S3 → AWS Glue → AWS Athena

---

## ⚙️ Tech Stack

- Python 🐍
- Apache Kafka
- Pandas
- AWS EC2 (Kafka Server)
- AWS S3 (Data Storage)
- AWS Glue (Data Catalog / ETL)
- AWS Athena (SQL Analytics)
- IAM Roles (Security & Access Management)

---

## 📡 Project Workflow

### 1. Data Producer (Kafka Producer)
- Reads stock market dataset using Pandas
- Simulates real-time streaming using random sampling
- Sends data to Kafka topic (`demo_testing2`)

### 2. Kafka Broker (EC2)
- Hosts Kafka server on AWS EC2 instance
- Handles real-time message streaming between producer and consumer

### 3. Data Consumer (Kafka Consumer)
- Consumes streaming data from Kafka topic
- Writes each record into AWS S3 as JSON files

### 4. AWS S3 Storage
- Stores streaming data in JSON format
- Acts as a data lake for analytics

### 5. AWS Glue
- Crawls S3 data
- Creates structured metadata tables

### 6. AWS Athena
- Runs SQL queries on processed data
- Enables real-time analytics on stock market dataset

---

## 🧪 Kafka Producer Code (Sample)

```python
from kafka import KafkaProducer
import pandas as pd
from json import dumps
from time import sleep

producer = KafkaProducer(
    bootstrap_servers=['<EC2-IP>:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

df = pd.read_csv("indexProcessed.csv")

while True:
    record = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_testing2', value=record)
    sleep(1)

