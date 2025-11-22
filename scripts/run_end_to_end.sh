#!/bin/bash
################################################################################
# Run Complete End-to-End Pipeline
# 
# Orchestrates the complete workflow:
# 1. Split large XML into chunks
# 2. Upload chunks to HDFS/DBFS
# 3. Run Spark ingestion pipeline
# 4. Validate output tables
################################################################################

set -e

echo "=========================================="
echo "XML2Hive - End-to-End Pipeline"
echo "=========================================="
echo ""

# Configuration - EDIT THESE VALUES
PLATFORM="yarn"  # "yarn" or "databricks"
INPUT_XML="/path/to/huge-file.xml"
LOCAL_CHUNKS_DIR="/tmp/xml_chunks"
REMOTE_CHUNKS_DIR="hdfs:///data/xml/chunks"  # or dbfs:/data/xml/chunks for Databricks
OUTPUT_DIR="hdfs:///data/output/tables"      # or dbfs:/data/output/tables
ROW_TAG="Record"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

################################################################################
# STEP 1: Build Project
################################################################################
echo "[Step 1/5] Building project..."
echo ""
"$SCRIPT_DIR/build.sh"
echo ""

################################################################################
# STEP 2: Split XML
################################################################################
echo "[Step 2/5] Splitting XML file..."
echo ""

# Create temporary directory
mkdir -p "$LOCAL_CHUNKS_DIR"

# Run splitter
java \
    -Xmx4g \
    -cp "$SCRIPT_DIR/../target/xml2hive.jar" \
    XmlStreamingSplitter \
    "$INPUT_XML" \
    "$LOCAL_CHUNKS_DIR" \
    "$ROW_TAG" \
    1000

CHUNK_COUNT=$(ls -1 "$LOCAL_CHUNKS_DIR"/*.xml 2>/dev/null | wc -l)
echo ""
echo "✓ Created $CHUNK_COUNT chunk files"
echo ""

################################################################################
# STEP 3: Upload Chunks
################################################################################
echo "[Step 3/5] Uploading chunks to remote storage..."
echo ""

if [[ "$REMOTE_CHUNKS_DIR" == hdfs://* ]]; then
    # HDFS
    echo "Uploading to HDFS..."
    hdfs dfs -mkdir -p "$REMOTE_CHUNKS_DIR"
    hdfs dfs -put -f "$LOCAL_CHUNKS_DIR"/*.xml "$REMOTE_CHUNKS_DIR/"
    UPLOADED_COUNT=$(hdfs dfs -ls "$REMOTE_CHUNKS_DIR"/*.xml | wc -l)
    echo "✓ Uploaded $UPLOADED_COUNT files to HDFS"
    
elif [[ "$REMOTE_CHUNKS_DIR" == dbfs://* ]]; then
    # Databricks DBFS
    echo "Uploading to DBFS..."
    databricks fs mkdirs "$REMOTE_CHUNKS_DIR"
    for file in "$LOCAL_CHUNKS_DIR"/*.xml; do
        databricks fs cp "$file" "$REMOTE_CHUNKS_DIR/" --overwrite
    done
    echo "✓ Uploaded to DBFS"
else
    echo "❌ ERROR: Unknown storage system: $REMOTE_CHUNKS_DIR"
    exit 1
fi

echo ""

################################################################################
# STEP 4: Run Spark Pipeline
################################################################################
echo "[Step 4/5] Running Spark ingestion pipeline..."
echo ""

if [ "$PLATFORM" == "yarn" ]; then
    export INPUT_PATH="$REMOTE_CHUNKS_DIR"
    export OUTPUT_PATH="$OUTPUT_DIR"
    export ROW_TAG="$ROW_TAG"
    "$SCRIPT_DIR/run_spark_yarn.sh"
elif [ "$PLATFORM" == "databricks" ]; then
    export INPUT_PATH="$REMOTE_CHUNKS_DIR"
    export OUTPUT_PATH="$OUTPUT_DIR"
    export ROW_TAG="$ROW_TAG"
    "$SCRIPT_DIR/run_spark_databricks.sh"
else
    echo "❌ ERROR: Unknown platform: $PLATFORM"
    exit 1
fi

echo ""

################################################################################
# STEP 5: Validate Output
################################################################################
echo "[Step 5/5] Validating output..."
echo ""

if [[ "$OUTPUT_DIR" == hdfs://* ]]; then
    echo "Output tables in HDFS:"
    hdfs dfs -ls "$OUTPUT_DIR"
    echo ""
    echo "Main table size:"
    hdfs dfs -du -h "$OUTPUT_DIR/main_table" | head -5
elif [[ "$OUTPUT_DIR" == dbfs://* ]]; then
    echo "Output tables in DBFS:"
    databricks fs ls "$OUTPUT_DIR"
fi

echo ""
echo "=========================================="
echo "✓ Pipeline Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Query tables with Spark SQL or Hive"
echo "  2. Create external tables pointing to Parquet files"
echo "  3. Run data quality checks"
echo ""
echo "Example query:"
echo "  spark-sql -e \"SELECT * FROM parquet.\`$OUTPUT_DIR/main_table\` LIMIT 10\""
echo ""

# Cleanup local chunks
read -p "Delete local chunks directory? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf "$LOCAL_CHUNKS_DIR"
    echo "✓ Local chunks deleted"
fi
