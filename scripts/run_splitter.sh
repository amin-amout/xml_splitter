#!/bin/bash
################################################################################
# Run XML Streaming Splitter
# 
# Splits a large XML file into smaller chunks for parallel Spark processing
################################################################################

set -e

# Configuration - EDIT THESE VALUES
INPUT_XML="/path/to/your/huge-file.xml"
OUTPUT_DIR="/path/to/output/chunks"
ROW_TAG="Record"  # The XML tag that represents one record
MAX_RECORDS_PER_FILE=1000

# Advanced options
JAVA_OPTS="-Xmx4g -Xms2g"  # 4GB max heap, 2GB initial

################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_FILE="$PROJECT_ROOT/target/xml2hive.jar"

echo "======================================"
echo "XML Streaming Splitter"
echo "======================================"
echo ""
echo "Input XML: $INPUT_XML"
echo "Output directory: $OUTPUT_DIR"
echo "Row tag: <$ROW_TAG>"
echo "Max records per file: $MAX_RECORDS_PER_FILE"
echo ""

# Validate inputs
if [ ! -f "$INPUT_XML" ]; then
    echo "❌ ERROR: Input file not found: $INPUT_XML"
    exit 1
fi

if [ ! -f "$JAR_FILE" ]; then
    echo "❌ ERROR: JAR file not found: $JAR_FILE"
    echo "   Please run: ./scripts/build.sh first"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Run splitter
echo "Starting XML splitter..."
echo ""

time java \
    $JAVA_OPTS \
    -cp "$JAR_FILE" \
    XmlStreamingSplitter \
    "$INPUT_XML" \
    "$OUTPUT_DIR" \
    "$ROW_TAG" \
    $MAX_RECORDS_PER_FILE

echo ""
echo "======================================"
echo "✓ Splitting Complete!"
echo "======================================"
echo ""
echo "Output files:"
ls -lh "$OUTPUT_DIR" | head -20
echo ""
echo "Total chunks created: $(ls -1 "$OUTPUT_DIR"/*.xml 2>/dev/null | wc -l)"
echo ""
