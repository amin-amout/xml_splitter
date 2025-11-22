#!/bin/bash
################################################################################
# Quick Test Script
# 
# Tests the complete pipeline with sample data
################################################################################

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "XML2Hive - Quick Test"
echo "=========================================="
echo ""

# Step 1: Build
echo "[1/4] Building project..."
./scripts/build.sh
echo ""

# Step 2: Run splitter on sample data
echo "[2/4] Testing XML splitter..."
mkdir -p test-chunks
java -Xmx2g -cp target/xml2hive.jar \
  XmlStreamingSplitter \
  examples/sample-data.xml \
  test-chunks \
  Order \
  2

echo ""
echo "✓ Generated chunks:"
ls -lh test-chunks/
echo ""

# Step 3: Validate XML chunks
echo "[3/4] Validating XML chunks..."
if command -v xmllint &> /dev/null; then
    for f in test-chunks/*.xml; do
        xmllint --noout "$f" && echo "  ✓ $f is valid"
    done
else
    echo "  ⚠ xmllint not found, skipping validation"
fi
echo ""

# Step 4: Test Spark job locally (if spark-submit available)
echo "[4/4] Testing Spark pipeline..."
if command -v spark-submit &> /dev/null; then
    mkdir -p test-output
    
    spark-submit \
      --master "local[4]" \
      --packages com.databricks:spark-xml_2.12:0.17.0 \
      --class XmlToParquetPipeline \
      target/xml2hive.jar \
      test-chunks \
      test-output \
      Order \
      10
    
    echo ""
    echo "✓ Output tables:"
    find test-output -name "*.parquet" | head -10
    
    echo ""
    echo "To inspect results:"
    echo "  spark-shell --packages com.databricks:spark-xml_2.12:0.17.0"
    echo "  scala> val df = spark.read.parquet(\"test-output/main_table\")"
    echo "  scala> df.show()"
else
    echo "  ⚠ spark-submit not found, skipping Spark test"
    echo "  Install Spark to run full test"
fi

echo ""
echo "=========================================="
echo "✓ Quick Test Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Review test-chunks/ for XML chunk quality"
echo "  2. Review test-output/ for Parquet tables"
echo "  3. Run on your actual 30GB XML file"
echo ""
