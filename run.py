import json
import os
import logging
import concurrent.futures
import boto3
import argparse
from botocore.config import Config
from pyspark.sql import SparkSession
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Collect Iceberg table metrics and push to Prometheus')

    # Required arguments
    parser.add_argument('--warehouse', required=True,
                        help='S3 Tables warehouse ARN (e.g., arn:aws:s3tables:us-east-1:123456789012:bucket/bucket-name)')

    # Optional arguments
    parser.add_argument('--region', default='us-east-1',
                        help='AWS region (default: us-east-1)')
    parser.add_argument('--spark-version', default='3.4',
                        help='Spark version (default: 3.4)')
    parser.add_argument('--iceberg-version', default='1.7.0',
                        help='Iceberg version (default: 1.7.0)')
    parser.add_argument('--prometheus-gateway', default='localhost:9091',
                        help='Prometheus Pushgateway address (default: localhost:9091)')
    parser.add_argument('--max-workers', type=int, default=1,
                        help='Maximum number of worker threads (default: 1)')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level (default: INFO)')

    return parser.parse_args()

def create_spark_session(args):
    """Create and configure Spark session"""

    spark = SparkSession.builder.appName("iceberg_lab") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.client.region", args.region) \
        .config("spark.sql.catalog.defaultCatalog", "s3tablesbucket") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", args.warehouse) \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.jars.packages",
                f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{args.iceberg_version},software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3,software.amazon.awssdk:glue:2.20.143,software.amazon.awssdk:sts:2.20.143,software.amazon.awssdk:s3:2.20.143,software.amazon.awssdk:dynamodb:2.20.143,software.amazon.awssdk:kms:2.20.143") \
        .getOrCreate()
    return spark


def get_s3tables_client(args):
    """Create S3Tables client"""
    return boto3.client('s3tables', region_name=args.region)


def get_table_metrics(spark, namespace, table):
    """Get essential table metrics using Spark SQL"""
    try:
        query = f"""
        SELECT 
            count(*) as partition_count,
            sum(file_count) as total_files,
            sum(total_data_file_size_in_bytes) as total_bytes,
            sum(record_count) as total_records
        FROM s3tablesbucket.{namespace}.{table}.partitions
        """

        metrics = spark.sql(query).collect()[0]

        return {
            "database": namespace,
            "table": table,
            "metrics": {
                "partition_count": metrics.partition_count,
                "file_count": metrics.total_files or 0,
                "total_bytes": metrics.total_bytes or 0,
                "total_records": metrics.total_records or 0,
                "avg_partition_size": (
                        metrics.total_bytes / metrics.partition_count) if metrics.partition_count > 0 else 0
            }
        }
    except Exception as e:
        logger.error(f"Error getting metrics for {namespace}.{table}: {e}")
        return None


def push_metrics_to_prometheus(metrics, prometheus_gateway):
    """Push metrics to Prometheus gateway"""
    try:
        registry = CollectorRegistry()

        # Define Prometheus metrics
        table_metrics = {
            'partition_count': Gauge('iceberg_table_partitions', 'Total partitions',
                                     ['database', 'table'], registry=registry),
            'file_count': Gauge('iceberg_table_files', 'Total files',
                                ['database', 'table'], registry=registry),
            'total_bytes': Gauge('iceberg_storage_total_bytes', 'Total bytes',
                                 ['database', 'table'], registry=registry),
            'total_records': Gauge('iceberg_table_records', 'Total records',
                                   ['database', 'table'], registry=registry),
            'avg_partition_size': Gauge('iceberg_table_avg_partition_size',
                                        'Average partition size in bytes',
                                        ['database', 'table'], registry=registry)
        }

        # Add table info metric for counting
        table_info = Gauge('iceberg_table_info', 'Iceberg table info',
                           ['database', 'table'], registry=registry)

        # Set labels
        labels = {'database': metrics['database'], 'table': metrics['table']}

        # Set table info metric for counting
        table_info.labels(**labels).set(1)

        # Set other metrics
        logger.info(f"Setting metrics for {labels}")
        for metric_name, value in metrics['metrics'].items():
            if metric_name in table_metrics:
                try:
                    float_value = float(value)
                    table_metrics[metric_name].labels(**labels).set(float_value)
                    logger.info(f"Set {metric_name} = {float_value}")
                except (TypeError, ValueError) as e:
                    logger.error(f"Error converting {metric_name} value {value}: {e}")
                    continue

        # Push to Prometheus with grouping key
        try:
            push_to_gateway(
                prometheus_gateway,
                job='iceberg_metrics',
                registry=registry,
                grouping_key={'database': metrics['database'], 'table': metrics['table']}
            )
            logger.info(f"✅ Successfully pushed metrics for {metrics['database']}.{metrics['table']}")
        except Exception as push_error:
            logger.error(f"Failed to push to Prometheus gateway: {push_error}")
            raise

    except Exception as e:
        logger.error(f"Error in push_metrics_to_prometheus: {str(e)}")
        raise


def process_table(spark, namespace, table, prometheus_gateway):
    """Process a single table"""
    try:
        metrics = get_table_metrics(spark, namespace, table)
        if metrics:
            print(json.dumps(metrics, indent=2))
            push_metrics_to_prometheus(metrics, prometheus_gateway)
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing {namespace}.{table}: {e}")
        return False


def list_namespaces(s3tables_client, warehouse_arn):
    """List namespaces"""
    try:
        namespaces = []
        paginator = s3tables_client.get_paginator('list_namespaces')
        for page in paginator.paginate(tableBucketARN=warehouse_arn):
            for namespace in page['namespaces']:
                # Extract string value from namespace list
                namespace_value = namespace['namespace']
                if isinstance(namespace_value, list):
                    namespace_value = namespace_value[0]
                namespaces.append(namespace_value)
        return namespaces
    except Exception as e:
        logger.error(f"Error listing namespaces: {e}")
        return []


def list_tables(s3tables_client, warehouse_arn, namespace):
    """List tables in namespace"""
    try:
        # Ensure namespace is a string
        namespace = namespace[0] if isinstance(namespace, list) else namespace

        tables = []
        paginator = s3tables_client.get_paginator('list_tables')
        for page in paginator.paginate(tableBucketARN=warehouse_arn, namespace=namespace):
            for table in page['tables']:
                tables.append(table['name'])
        return tables
    except Exception as e:
        logger.error(f"Error listing tables for {namespace}: {e}")
        return []


def main():
    # Parse command line arguments
    args = parse_args()

    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    spark = None
    try:
        # Initialize services
        spark = create_spark_session(args)
        spark.sparkContext.setLogLevel("ERROR")
        s3tables_client = get_s3tables_client(args)
        logger.info("✅ Initialized services")
        logger.info(f"Using warehouse: {args.warehouse}")

        # Process namespaces
        namespaces = list_namespaces(s3tables_client, args.warehouse)
        logger.info(f"Found namespaces: {namespaces}")

        for namespace in namespaces:
            logger.info(f"Processing namespace: {namespace}")

            tables = list_tables(s3tables_client, args.warehouse, namespace)
            logger.info(f"Found tables in {namespace}: {tables}")

            if not tables:
                continue

            # Process tables using thread pool
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                futures = [
                    executor.submit(process_table, spark, namespace, table, args.prometheus_gateway)
                    for table in tables
                ]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(f"Thread execution error: {exc}")

    except Exception as e:
        logger.error(f"Main execution error: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("✅ Stopped Spark session")


if __name__ == "__main__":
    main()