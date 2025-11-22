#!/bin/bash
################################################################################
# Run Spark Ingestion Pipeline on Databricks
# 
# Submits the PySpark job to Databricks cluster
################################################################################

set -e

# Configuration - EDIT THESE VALUES
DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN="your-token-here"  # Or use env var: $DATABRICKS_TOKEN
CLUSTER_ID="your-cluster-id"         # Get from Databricks UI

INPUT_PATH="dbfs:/data/xml/chunks"
OUTPUT_PATH="dbfs:/data/output/tables"
ROW_TAG="Record"
REPARTITION_SIZE=200

################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_SCRIPT="$PROJECT_ROOT/src/main/python/xml_to_parquet_pipeline.py"

echo "======================================"
echo "Spark Pipeline - Databricks"
echo "======================================"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ ERROR: Databricks CLI not found"
    echo ""
    echo "Install with: pip install databricks-cli"
    echo "Configure with: databricks configure --token"
    exit 1
fi

# Validate Python script
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "❌ ERROR: Python script not found: $PYTHON_SCRIPT"
    exit 1
fi

echo "Input path: $INPUT_PATH"
echo "Output path: $OUTPUT_PATH"
echo "Row tag: <$ROW_TAG>"
echo "Cluster ID: $CLUSTER_ID"
echo ""

# Upload Python script to DBFS
echo "[1/3] Uploading Python script to DBFS..."
databricks fs cp "$PYTHON_SCRIPT" dbfs:/scripts/ --overwrite
echo "✓ Script uploaded"
echo ""

# Create notebook or run as Python task
echo "[2/3] Creating job..."

JOB_NAME="XML to Parquet Pipeline - $(date +%Y%m%d_%H%M%S)"

# Create job JSON
cat > /tmp/databricks_job.json <<EOF
{
  "name": "$JOB_NAME",
  "new_cluster": {
    "spark_version": "12.2.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 10,
    "spark_conf": {
      "spark.sql.shuffle.partitions": "$SPARK_SHUFFLE_PARTITIONS",
      "spark.sql.adaptive.enabled": "true",
      "spark.memory.fraction": "0.8"
    }
  },
  "libraries": [
    {
      "maven": {
        "coordinates": "com.databricks:spark-xml_2.12:0.17.0"
      }
    }
  ],
  "spark_python_task": {
    "python_file": "dbfs:/scripts/xml_to_parquet_pipeline.py",
    "parameters": [
      "$INPUT_PATH",
      "$OUTPUT_PATH",
      "$ROW_TAG",
      "$REPARTITION_SIZE"
    ]
  }
}
EOF

# Submit job
echo "[3/3] Submitting job to Databricks..."
RUN_ID=$(databricks jobs create --json-file /tmp/databricks_job.json | jq -r '.job_id')

if [ -z "$RUN_ID" ]; then
    echo "❌ ERROR: Failed to create job"
    exit 1
fi

echo "✓ Job created: ID $RUN_ID"
echo ""

# Run the job
databricks jobs run-now --job-id "$RUN_ID"

echo ""
echo "======================================"
echo "✓ Job Submitted to Databricks"
echo "======================================"
echo ""
echo "Monitor job progress:"
echo "  $DATABRICKS_HOST/#job/$RUN_ID"
echo ""
echo "Or use CLI:"
echo "  databricks runs list --job-id $RUN_ID"
echo ""

rm -f /tmp/databricks_job.json
