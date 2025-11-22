#!/bin/bash
################################################################################
# Maven Build Script for XML2Hive
# 
# This script uses Maven to compile Scala code and package into a JAR
################################################################################

set -e  # Exit on error

echo "======================================"
echo "XML2Hive - Maven Build"
echo "======================================"
echo ""

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"
echo ""

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo "❌ ERROR: Maven (mvn) not found. Please install Maven."
    echo ""
    echo "Installation options:"
    echo "  - macOS: brew install maven"
    echo "  - Debian/Ubuntu: sudo apt-get install maven"
    echo "  - RHEL/CentOS: sudo yum install maven"
    echo "  - Manual: Download from https://maven.apache.org/download.cgi"
    echo ""
    exit 1
fi

echo "✓ Found Maven: $(mvn -version | head -1)"
echo ""

# Check if pom.xml exists
if [ ! -f "$PROJECT_ROOT/pom.xml" ]; then
    echo "❌ ERROR: pom.xml not found in $PROJECT_ROOT"
    exit 1
fi

echo "[1/3] Cleaning previous build..."
mvn clean
echo ""

echo "[2/3] Compiling and packaging..."
mvn package -DskipTests
echo ""

# Check if JAR was created
JAR_FILE="$PROJECT_ROOT/target/xml2hive.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "❌ ERROR: JAR file not created"
    exit 1
fi

echo "[3/3] Build Summary"
echo "======================================"
JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
echo "✓ Build Complete!"
echo "======================================"
echo "JAR file: $JAR_FILE"
echo "JAR size: $JAR_SIZE"
echo ""
echo "JAR Contents (main classes):"
jar tf "$JAR_FILE" | grep -E "\.class$" | grep -v '\$' | head -10
echo ""
echo "Next steps:"
echo "  1. Test locally:"
echo "     java -cp target/xml2hive.jar XmlStreamingSplitter examples/sample-data.xml test-chunks Order 2"
echo ""
echo "  2. Run Spark job:"
echo "     spark-submit --class XmlToParquetPipeline target/xml2hive.jar <input> <output> <rowTag>"
echo ""
echo "  3. Upload to cluster:"
echo "     hdfs dfs -put target/xml2hive.jar /tmp/jars/"
echo ""
