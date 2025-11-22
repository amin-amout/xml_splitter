# Troubleshooting Guide

Common issues and solutions for the XML2Hive pipeline.

---

## Table of Contents

1. [OutOfMemory Errors](#outofmemory-errors)
2. [Performance Issues](#performance-issues)
3. [Schema Problems](#schema-problems)
4. [File System Issues](#file-system-issues)
5. [Spark Job Failures](#spark-job-failures)
6. [Data Quality Issues](#data-quality-issues)

---

## OutOfMemory Errors

### XML Splitter OOM

**Symptom:**
```
Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
```

**Causes:**
- Individual XML records are too large (>100MB each)
- Java heap too small for record buffer

**Solutions:**

1. **Increase heap size:**
```bash
scala -Xmx8g -Xms4g -cp xml2hive.jar XmlStreamingSplitter ...
```

2. **Check individual record size:**
```bash
# Find largest records
grep -o '<Record[^>]*>' huge-file.xml | head -1000 | while read line; do
    # Extract one record and check size
    echo "$line" | wc -c
done | sort -n | tail -10
```

3. **If records are genuinely massive (>500MB each):**
   - Split at a deeper nested level
   - Use a different `rowTag` that represents smaller units
   - Consider preprocessing with `sed` or `awk` to split files

### Spark Executor OOM

**Symptom:**
```
ExecutorLostFailure (executor 2 exited caused by one or more user class threw OutOfMemoryError)
```

**Solutions:**

1. **Increase executor memory:**
```bash
--executor-memory 12G \
--conf spark.executor.memoryOverhead=2G
```

2. **Reduce partition size:**
```bash
# More partitions = less data per task
--conf spark.sql.shuffle.partitions=600
--conf spark.sql.files.maxPartitionBytes=67108864  # 64MB
```

3. **Reduce records per XML chunk:**
```bash
# In Stage 1, use smaller chunks
XmlStreamingSplitter ... Record 500  # Instead of 1000
```

4. **Enable off-heap memory:**
```bash
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=4g
```

### Spark Driver OOM

**Symptom:**
```
Driver OutOfMemoryError
```

**Causes:**
- Using `.collect()` or `.toPandas()`
- Schema inference on huge dataset
- Too many partitions reported back to driver

**Solutions:**

1. **Increase driver memory:**
```bash
--driver-memory 8G \
--conf spark.driver.maxResultSize=4g
```

2. **Avoid driver-side operations:**
```scala
// ❌ BAD - brings data to driver
val results = df.collect()

// ✅ GOOD - stays distributed
df.write.parquet(outputPath)
```

3. **Limit schema sampling:**
```scala
.option("samplingRatio", 0.01)  // Only sample 1% for schema
```

---

## Performance Issues

### Slow XML Splitting

**Symptom:** Splitter takes hours for 30GB file

**Solutions:**

1. **Check disk I/O:**
```bash
# Monitor while running
iostat -x 5
```

2. **Use local SSD instead of network storage**

3. **Increase buffer sizes:**
```scala
// In XmlStreamingSplitter.scala
val inputStream = new BufferedInputStream(
    new FileInputStream(config.inputFile), 
    16 * 1024 * 1024  // 16MB buffer
)
```

### Slow Spark Job

**Symptom:** Spark job takes 4+ hours for 30GB

**Diagnosis:**

1. **Check Spark UI:**
   - Are tasks skewed? (some take much longer)
   - Are there many shuffle operations?
   - Is GC time > 10% of task time?

2. **Check executor utilization:**
```bash
# In Spark UI, check "Executors" tab
# Look for idle executors or high GC time
```

**Solutions:**

1. **Enable adaptive query execution:**
```bash
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.sql.adaptive.skewJoin.enabled=true
```

2. **Optimize shuffle partitions:**
```bash
# Rule of thumb: 2-3× total cores
# 10 executors × 4 cores = 40 cores → 100-120 partitions
--conf spark.sql.shuffle.partitions=100
```

3. **Enable dynamic allocation:**
```bash
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=50
```

4. **Use broadcast joins for small tables:**
```scala
// If you're joining with dimension tables
df.join(broadcast(smallTable), "key")
```

### Too Many Small Files

**Symptom:** Output directory has 10,000+ tiny Parquet files

**Impact:** Slow reads, NameNode pressure

**Solutions:**

1. **Repartition before write:**
```scala
df.repartition(200).write.parquet(outputPath)
```

2. **Use coalesce for fewer files:**
```scala
df.coalesce(50).write.parquet(outputPath)
```

3. **Post-process with Spark:**
```scala
// Read and rewrite with fewer partitions
spark.read.parquet(inputPath)
  .coalesce(100)
  .write.mode("overwrite")
  .parquet(outputPath + "_optimized")
```

4. **Enable file consolidation:**
```bash
--conf spark.sql.files.maxRecordsPerFile=100000
```

---

## Schema Problems

### Schema Mismatch

**Symptom:**
```
org.apache.spark.sql.AnalysisException: cannot resolve 'CustomerID' given input columns...
```

**Cause:** XML attributes vs elements confusion

**Solutions:**

1. **Check actual schema:**
```scala
// In Spark job
rawDf.printSchema()
rawDf.show(5, truncate = false)
```

2. **Remember attribute naming:**
```xml
<Customer CustomerID="C001">  → col("_CustomerID")
<CustomerID>C001</CustomerID>  → col("CustomerID")
```

3. **List all columns:**
```scala
rawDf.columns.foreach(println)
```

### Corrupt Records

**Symptom:**
```
_corrupt_record column contains values
```

**Diagnosis:**
```scala
val corrupt = df.filter(col("_corrupt_record").isNotNull)
corrupt.show(truncate = false)
```

**Solutions:**

1. **Use PERMISSIVE mode (default):**
```scala
.option("mode", "PERMISSIVE")  // Keeps corrupt records
```

2. **Or drop them:**
```scala
.option("mode", "DROPMALFORMED")  // Discards corrupt records
```

3. **Fix XML chunks:**
```bash
# Validate chunks
for f in chunks/*.xml; do
    xmllint --noout "$f" 2>&1 | grep -v "validates" && echo "Invalid: $f"
done
```

### Nested Arrays Not Flattening

**Symptom:** Array columns remain unexploded

**Solution:**
```scala
// Explicit explode
val flattened = df.select(
  col("record_id"),
  explode_outer(col("Items.Item")).as("item")
).select(
  col("record_id"),
  col("item.*")  // Flatten struct
)
```

---

## File System Issues

### HDFS Permission Denied

**Symptom:**
```
org.apache.hadoop.security.AccessControlException: Permission denied
```

**Solutions:**

1. **Check HDFS permissions:**
```bash
hdfs dfs -ls /data/output/
```

2. **Set correct permissions:**
```bash
hdfs dfs -chmod -R 755 /data/output/
```

3. **Run as correct user:**
```bash
# In YARN
--proxy-user hdfs_user
```

### DBFS Access Issues

**Symptom:**
```
java.io.FileNotFoundException: dbfs:/data/ does not exist
```

**Solutions:**

1. **Create directory first:**
```bash
databricks fs mkdirs dbfs:/data/xml/chunks
```

2. **Use correct path format:**
```python
# Databricks notebook
dbutils.fs.ls("dbfs:/data/")
```

### Disk Space Full

**Symptom:**
```
No space left on device
```

**Solutions:**

1. **Check space:**
```bash
df -h
hdfs dfsadmin -report
```

2. **Clean up intermediate files:**
```bash
hdfs dfs -rm -r /tmp/spark-staging/
hdfs dfs -rm -r /user/$USER/.sparkStaging/
```

3. **Use compression:**
```scala
.option("compression", "snappy")  // Or "gzip" for more compression
```

---

## Spark Job Failures

### Application Killed by YARN

**Symptom:**
```
Application killed by ResourceManager
```

**Causes:**
- Requested more resources than available
- Executor memory + overhead > node memory

**Solutions:**

1. **Check YARN capacity:**
```bash
yarn node -list
yarn queue -status default
```

2. **Reduce resource requirements:**
```bash
--num-executors 5 \
--executor-memory 6G \
--executor-cores 3
```

3. **Set memory overhead:**
```bash
--conf spark.executor.memoryOverhead=1G
```

### Task Failures / Retries

**Symptom:** Many task retries in Spark UI

**Solutions:**

1. **Check for data skew:**
```scala
// Add salt to skewed keys
df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
```

2. **Increase task timeout:**
```bash
--conf spark.network.timeout=600s
--conf spark.executor.heartbeatInterval=60s
```

3. **Enable speculation:**
```bash
--conf spark.speculation=true
--conf spark.speculation.multiplier=2
```

### spark-xml Package Not Found

**Symptom:**
```
java.lang.ClassNotFoundException: com.databricks.spark.xml
```

**Solutions:**

1. **Add package to spark-submit:**
```bash
--packages com.databricks:spark-xml_2.12:0.17.0
```

2. **Or download JAR manually:**
```bash
wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.17.0/spark-xml_2.12-0.17.0.jar
--jars spark-xml_2.12-0.17.0.jar
```

---

## Data Quality Issues

### Missing / Null Values

**Symptom:** Many NULL values in output tables

**Diagnosis:**
```sql
SELECT 
  COUNT(*) as total,
  COUNT(customer_id) as non_null_customers,
  COUNT(*) - COUNT(customer_id) as null_customers
FROM main_table;
```

**Solutions:**

1. **Use coalesce for defaults:**
```scala
col("OptionalField").coalesce(lit("UNKNOWN"))
```

2. **Filter out nulls:**
```scala
df.filter(col("customer_id").isNotNull)
```

### Duplicate Records

**Symptom:** More records than expected

**Diagnosis:**
```sql
SELECT id, COUNT(*) as cnt
FROM main_table
GROUP BY id
HAVING cnt > 1;
```

**Solutions:**

1. **Deduplicate:**
```scala
df.dropDuplicates("id")

// Or keep first/last
df.groupBy("id")
  .agg(first("name").as("name"), ...)
```

### Type Conversion Errors

**Symptom:**
```
java.lang.NumberFormatException: For input string: "N/A"
```

**Solutions:**

1. **Safe casting:**
```scala
// Use try_cast in SQL (Spark 3.2+)
expr("try_cast(Amount as decimal(18,2))")

// Or handle nulls
when(col("Amount").cast("decimal").isNull, lit(0.0))
  .otherwise(col("Amount").cast("decimal"))
```

2. **Clean data first:**
```scala
df.withColumn(
  "amount_clean",
  regexp_replace(col("Amount"), "[^0-9.]", "")
).withColumn(
  "amount_decimal",
  col("amount_clean").cast("decimal(18,2)")
)
```

---

## Debugging Tips

### Enable Verbose Logging

```bash
# In Spark
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j.properties"
```

### Check Spark Logs

```bash
# YARN
yarn logs -applicationId application_1234567890_0001

# Databricks
# Go to Cluster → Event Log → Download
```

### Profile Spark Job

```bash
# Add metrics
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs:///spark-history

# View in Spark History Server
```

### Validate XML Chunks

```bash
# Check if chunks are valid XML
find chunks/ -name "*.xml" -exec xmllint --noout {} \; 2>&1 | grep -v "validates"

# Check chunk sizes
du -sh chunks/*
```

### Test with Small Sample

```bash
# Test with just a few chunks
hdfs dfs -mkdir test-chunks
hdfs dfs -put chunks/part-0000[1-5].xml test-chunks/

# Run Spark job on sample
spark-submit ... test-chunks test-output Record
```

---

## Performance Tuning Checklist

- [ ] XML chunks are 50-100MB each
- [ ] Shuffle partitions = 2-3× total cores
- [ ] Executor memory sufficient (8GB+ recommended)
- [ ] Adaptive query execution enabled
- [ ] Compression enabled (snappy)
- [ ] No `.collect()` or driver-side operations
- [ ] Output files consolidated (not 1000s of tiny files)
- [ ] Dynamic allocation enabled
- [ ] Broadcast joins used where applicable
- [ ] Data skew addressed if present

---

## Getting Help

If issues persist:

1. **Check Spark UI** for task-level details
2. **Review executor logs** for OOM or exceptions
3. **Validate input data** with `xmllint`
4. **Test with smaller dataset** first
5. **Monitor resource usage** with `top`, `htop`, or YARN UI

---

**Remember:** The pipeline is designed to be memory-safe. If you encounter OOM, it's usually a configuration issue, not a fundamental design flaw.
