# Kafka–Spark Streaming Pipeline → PostgreSQL

A real-time data streaming pipeline that ingests user behavior logs from **Apache Kafka**, processes them using **Apache Spark Structured Streaming**, and stores cleaned, transformed results into a **PostgreSQL** database for analytics and reporting.

---

## Overview

This project simulates an **e-commerce clickstream analytics pipeline**, where user actions (such as viewing product details) are streamed in real time from Kafka, transformed in Spark, and stored in a star-schema database model for downstream reporting and analysis.

---

## Architecture

```text
Kafka (SASL_PLAINTEXT)
       ↓
Spark Structured Streaming
       ↓
+------------------------------+
|   Transformations            |
|   ├─ dim_product             |
|   ├─ dim_referrer            |
|   ├─ dim_user_agent          |
|   ├─ dim_time                |
|   └─ fact_views              |
+------------------------------+
       ↓
PostgreSQL (Data Warehouse)

