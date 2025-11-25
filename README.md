
<img width="1191" height="1161" alt="realTimeStreamingArch drawio" src="https://github.com/user-attachments/assets/e06a5a93-1765-4826-a3a4-190da4f22cac" />


## Architecture Overview

This project implements a real-time streaming data pipeline using:
- **Apache Kafka**: Distributed message broker for data ingestion
- **Apache Spark Structured Streaming**: Real-time processing engine
- **HDFS**: Distributed storage for static data
- **GCP Dataproc**: Managed Spark cluster infrastructure

### Pipeline Types

1. **Stateless Processing**: Filtering and transformation of user events
2. **Stateful Aggregation**: Time-windowed aggregations on transaction data
3. **Stream-Static Join**: Enriching streaming data with static dimensions

### Key Features
- Micro-batch processing with configurable intervals
- Fault tolerance via Kafka offset management
- Horizontal scalability with partitioned topics
