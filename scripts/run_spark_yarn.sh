#!/bin/bash
################################################################################
# Run Spark Ingestion Pipeline on YARN
# 
# Submits the Spark job to process XML chunks and create Parquet tables
################################################################################

set -e

# Configuration - EDIT THESE VALUES
INPUT_PATH="hdfs:///data/xml/chunks"         # Path to XML chunks
OUTPUT_PATH="hdfs:///data/output/tables"     # Output path for Parquet tables
ROW_TAG="Record"                             # XML record tag
REPARTITION_SIZE=200                         # Number of output partitions

# YARN/Spark Configuration
MASTER="yarn"
DEPLOY_MODE="cluster"  # or "client" for local testing
EXECUTOR_MEMORY="8g"
EXECUTOR_CORES=4
NUM_EXECUTORS=10
DRIVER_MEMORY="4g"
SPARK_SHUFFLE_PARTITIONS=400

# Spark-XML version
SPARK_XML_VERSION="0.17.0"
SCALA_VERSION="2.12"

################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_FILE="$PROJECT_ROOT/target/xml2hive.jar"

echo "======================================"
echo "Spark XML Ingestion Pipeline (YARN)"
echo "======================================"
echo ""
echo "Input path: $INPUT_PATH"
echo "Output path: $OUTPUT_PATH"
echo "Row tag: <$ROW_TAG>"
echo "Master: $MASTER"
echo "Deploy mode: $DEPLOY_MODE"
echo ""

# Validate JAR
if [ ! -f "$JAR_FILE" ]; then
    echo "❌ ERROR: JAR file not found: $JAR_FILE"
    echo "   Please run: ./scripts/build.sh first"
    exit 1
fi

# Upload JAR to HDFS if in cluster mode
if [ "$DEPLOY_MODE" == "cluster" ]; then
    echo "Uploading JAR to HDFS..."
    hdfs dfs -mkdir -p /tmp/jars
    hdfs dfs -put -f "$JAR_FILE" /tmp/jars/
    JAR_FILE="hdfs:///tmp/jars/xml2hive.jar"
    echo "✓ JAR uploaded"
    echo ""
fi

# Submit Spark job
echo "Submitting Spark job..."
echo ""

spark-submit \
    --master "$MASTER" \
    --deploy-mode "$DEPLOY_MODE" \
    --class XmlToParquetPipeline \
    --name "XML to Parquet Pipeline" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores $EXECUTOR_CORES \
    --num-executors $NUM_EXECUTORS \
    --driver-memory "$DRIVER_MEMORY" \
    --conf spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS \
    --conf spark.memory.fraction=0.8 \
    --conf spark.memory.storageFraction=0.3 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --conf spark.sql.files.maxPartitionBytes=134217728 \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=60s \
    --packages "com.databricks:spark-xml_${SCALA_VERSION}:${SPARK_XML_VERSION}" \
    "$JAR_FILE" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$ROW_TAG" \
    $REPARTITION_SIZE

echo ""
echo "======================================"
echo "✓ Spark Job Submitted"
echo "======================================"
echo ""
echo "Monitor job progress:"
echo "  - YARN UI: http://<resource-manager>:8088"
echo "  - Spark UI: Check YARN application logs"
echo ""
echo "When complete, verify output:"
echo "  hdfs dfs -ls $OUTPUT_PATH"
echo ""
