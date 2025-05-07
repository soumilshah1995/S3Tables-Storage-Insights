# S3Tables Storage Insights

A monitoring solution for AWS S3Tables that collects, analyzes, and visualizes storage metrics.

![Image](https://github.com/user-attachments/assets/cfd2c980-17a7-47e7-8de3-8266de0336ce)

## Overview

S3Tables Storage Insights helps you track and visualize storage metrics for your AWS S3Tables data. The solution collects comprehensive metrics including:

- Total number of databases
- Total number of tables across all namespaces
- Total storage usage per namespace
- Storage growth trends
- Top 5 tables by record count
- Top 5 tables by size (GB/TB)

## Deployment

### Prerequisites

- Docker and Docker Compose
- AWS credentials configured
- Access to S3Tables resources

### Installation

Deploy the solution with Docker Compose:

```bash
docker compose up --build -d
```

### Running the Collection Job

Execute the metric collection job:

```bash
python run.py \
  --warehouse "arn:aws:s3tables:us-east-1:XX:bucket/X" \
  --region "us-east-1" \
  --prometheus-gateway "localhost:9091" \
  --max-workers 4 \
  --log-level "INFO"
```

## Metrics Collected

The job captures detailed metrics for each table:

```json
{
  "database": "<database_name>",
  "table": "<table_name>",
  "metrics": {
    "partition_count": 2,
    "file_count": 2,
    "total_bytes": 1333,
    "total_records": 9,
    "avg_partition_size": 666.5
  }
}
```

## Viewing Results

Access the Grafana dashboard at:

```
http://localhost:3000/d/iceberg-storage-s3s/iceberg-storage-overview?orgId=1&from=now-6h&to=now&timezone=browser&refresh=1m
```

## Customization

You can extend the solution to collect and visualize additional metrics based on your specific requirements.
