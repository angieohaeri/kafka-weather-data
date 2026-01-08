# Kafka Weather Data: Project Overview

Implements a Kafka-based pipeline where weather data is produced by a Python program, streamed through Kafka, and consumed to generate partitioned JSON files with summary statistics suitable for a web dashboard.

## Key Features
- Kafka producer and consumer implementation in Python
- Streaming with manual and automatic partition assignment
- Exactly-once semantics and crash recovery
- Atomic writes for JSON output

## Notes on Implementation

- **Implemented by me:** `producer.py`, `debug.py`, `consumer.py`, Dockerfile, crash recovery logic, atomic JSON writes.  
- **Provided files:** `weather.py` (simulated weather generator), `report.proto` (protobuf schema).  

## Academic Context and Notes on Provided Code

This project was completed as part of a university course on distributed big data systems and data engineering. Some utility scripts and configuration files were provided as scaffolding for the project.
