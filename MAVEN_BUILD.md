# Building with Maven

## Prerequisites

- **Maven 3.6+** (check with `mvn -version`)
- **Java 8+** (check with `java -version`)

Maven will automatically download Scala compiler and all dependencies.

## Quick Build

```bash
# Clean and build
mvn clean package

# Skip tests (faster)
mvn clean package -DskipTests

# Build with verbose output
mvn clean package -X
```

## Output

After successful build, you'll find:
- `target/xml2hive.jar` - **Main JAR with all dependencies (5.2MB) - USE THIS ONE**
- `target/xml2hive-1.0-SNAPSHOT.jar` - Thin JAR without dependencies (29KB)

## Run the Splitter

```bash
# Test with sample data
scala -cp target/xml2hive.jar XmlStreamingSplitter \
  examples/sample-data.xml \
  test-chunks \
  Order \
  2

# Or use the script
./scripts/run_splitter.sh
```

## Run Spark Job

```bash
# Local mode
spark-submit \
  --master local[4] \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --class XmlToParquetPipeline \
  target/xml2hive.jar \
  test-chunks \
  test-output \
  Order

# YARN cluster mode
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 10 \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --class XmlToParquetPipeline \
  target/xml2hive.jar \
  hdfs:///data/chunks \
  hdfs:///data/output \
  Record
```

## Maven Lifecycle Commands

| Command | Description |
|---------|-------------|
| `mvn clean` | Remove target/ directory |
| `mvn compile` | Compile Scala sources |
| `mvn package` | Create JAR file |
| `mvn install` | Install to local Maven repo |
| `mvn dependency:tree` | Show dependency tree |
| `mvn dependency:copy-dependencies` | Copy all dependencies |

## Customizing pom.xml

### Change Scala Version

```xml
<properties>
    <scala.version>2.12.18</scala.version>
</properties>
```

### Change Spark Version

```xml
<properties>
    <spark.version>3.5.0</spark.version>
</properties>
```

### Add More Dependencies

```xml
<dependencies>
    <dependency>
        <groupId>com.typesafe</groupId>
        <artifactId>config</artifactId>
        <version>1.4.2</version>
    </dependency>
</dependencies>
```

## IDE Integration

### IntelliJ IDEA

1. File → Open → Select `pom.xml`
2. Right-click project → "Add Framework Support" → Scala
3. Code completion and refactoring will work

### VS Code

1. Install "Metals" extension
2. Open project folder
3. Metals will import Maven project automatically

### Eclipse

1. Import → Maven → Existing Maven Projects
2. Select project root
3. Install Scala IDE plugin if needed

## Troubleshooting

### Maven not found

```bash
# Ubuntu/Debian
sudo apt-get install maven

# macOS
brew install maven

# RHEL/CentOS
sudo yum install maven
```

### Scala compilation fails

```bash
# Clear Maven cache
rm -rf ~/.m2/repository/net/alchim31/maven/scala-maven-plugin

# Clear target
mvn clean

# Rebuild
mvn package
```

### OutOfMemory during build

```bash
# Increase Maven heap
export MAVEN_OPTS="-Xmx2g -Xms512m"
mvn clean package
```

## Fast Development Workflow

```bash
# Terminal 1: Continuous compilation
mvn scala:cc

# Terminal 2: Run after each compile
scala -cp target/classes:~/.m2/repository/org/scala-lang/scala-library/2.12.18/scala-library-2.12.18.jar \
  XmlStreamingSplitter examples/sample-data.xml test-chunks Order 2
```

## Production Deployment

```bash
# 1. Build release
mvn clean package -DskipTests

# 2. Upload to cluster
scp target/xml2hive.jar your-cluster:/path/to/jars/

# 3. Or upload to HDFS
hdfs dfs -put target/xml2hive.jar /user/hdfs/jars/

# 4. Run on cluster
spark-submit --class XmlToParquetPipeline /path/to/xml2hive.jar ...
```

## Alternative: Skip Maven, Use Scripts

If you prefer not to use Maven, you can still compile with the original scripts:

1. Install Scala: `sudo apt-get install scala`
2. Run: `scalac -d target/classes src/main/scala/*.scala`
3. Package: `jar cvf target/xml2hive.jar -C target/classes .`

But Maven is recommended for:
- ✅ Dependency management
- ✅ IDE integration
- ✅ Reproducible builds
- ✅ Easier team collaboration
