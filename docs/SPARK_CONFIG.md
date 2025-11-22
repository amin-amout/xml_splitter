# Spark Configuration Reference

Recommended Spark configurations for different cluster sizes and data volumes.

---

## Configuration Profiles

### Small Dataset (1-5GB)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4G \
  --executor-cores 2 \
  --num-executors 5 \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=50 \
  --conf spark.memory.fraction=0.8 \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --class XmlToParquetPipeline \
  xml2hive.jar \
  <input> <output> <rowTag>
```

**Total resources:** 5 executors × 4GB = 20GB

---

### Medium Dataset (10-30GB) - **RECOMMENDED FOR YOUR USE CASE**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 10 \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.executor.memoryOverhead=1G \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=134217728 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=60s \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --class XmlToParquetPipeline \
  xml2hive.jar \
  <input> <output> <rowTag>
```

**Total resources:** 10-20 executors × 8GB = 80-160GB

---

### Large Dataset (50-100GB)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 12G \
  --executor-cores 5 \
  --num-executors 20 \
  --driver-memory 8G \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.2 \
  --conf spark.executor.memoryOverhead=2G \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=10 \
  --conf spark.dynamicAllocation.maxExecutors=40 \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.reducer.maxSizeInFlight=96m \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  --class XmlToParquetPipeline \
  xml2hive.jar \
  <input> <output> <rowTag>
```

**Total resources:** 20-40 executors × 12GB = 240-480GB

---

## Configuration Explained

### Memory Settings

| Parameter | Description | Recommendation |
|-----------|-------------|----------------|
| `--executor-memory` | Heap memory per executor | 8-16GB for 30GB dataset |
| `--driver-memory` | Driver heap memory | 4-8GB (doesn't need to be huge) |
| `spark.executor.memoryOverhead` | Off-heap memory (network buffers, etc.) | 10-15% of executor memory |
| `spark.memory.fraction` | % of heap for execution/storage | 0.8 (default 0.6) |
| `spark.memory.storageFraction` | % of memory.fraction for caching | 0.2-0.3 (we don't cache much) |

### Parallelism Settings

| Parameter | Description | Recommendation |
|-----------|-------------|----------------|
| `--num-executors` | Number of executor JVMs | 10-20 for 30GB |
| `--executor-cores` | Cores per executor | 4-5 (not too high) |
| `spark.sql.shuffle.partitions` | Partitions for shuffles | 2-3× total cores |
| `spark.sql.files.maxPartitionBytes` | Max size per file partition | 128MB (default), 64MB for many small files |

**Why 4-5 cores per executor?**
- More cores = more concurrent tasks
- But too many cores = GC overhead
- 4-5 is sweet spot for memory:core ratio

**Calculating shuffle partitions:**
```
Total cores = num_executors × executor_cores
            = 10 × 4 = 40 cores
Shuffle partitions = 2-3× = 80-120 (round to 100-200)
```

### Adaptive Query Execution (AQE)

| Parameter | Description | Benefit |
|-----------|-------------|---------|
| `spark.sql.adaptive.enabled` | Enable AQE | Automatically optimizes at runtime |
| `spark.sql.adaptive.coalescePartitions.enabled` | Merge small partitions | Reduces small file problem |
| `spark.sql.adaptive.skewJoin.enabled` | Handle skewed joins | Splits large partitions |

**Recommendation:** Enable all AQE features for production workloads

### Dynamic Allocation

| Parameter | Description | Recommendation |
|-----------|-------------|----------------|
| `spark.dynamicAllocation.enabled` | Scale executors up/down | `true` for shared clusters |
| `spark.dynamicAllocation.minExecutors` | Minimum executors | 50% of num-executors |
| `spark.dynamicAllocation.maxExecutors` | Maximum executors | 2× num-executors |

**When to use:**
- ✅ Shared YARN cluster
- ✅ Variable workload
- ❌ Dedicated cluster (use fixed executors)

### Network / Timeout Settings

| Parameter | Description | Recommendation |
|-----------|-------------|----------------|
| `spark.network.timeout` | General network timeout | 600s (10 min) for large shuffles |
| `spark.executor.heartbeatInterval` | Executor → driver heartbeat | 60s (default) |
| `spark.rpc.askTimeout` | RPC timeout | Use network.timeout |

**Increase if you see:** `Heartbeat timeout`, `Network timeout`

---

## Databricks Configuration

For Databricks notebooks/jobs:

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
```

Or in cluster configuration (Spark Config tab):
```
spark.sql.shuffle.partitions 200
spark.sql.adaptive.enabled true
spark.memory.fraction 0.8
```

**Databricks Runtime:**
- Use DBR 12.2 LTS or later
- Photon acceleration (optional, for faster parquet writes)
- Instance type: `i3.xlarge` or `r5.2xlarge`

---

## Environment-Specific Examples

### AWS EMR

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 10 \
  --driver-memory 4G \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  s3://my-bucket/jars/xml2hive.jar \
  s3://my-bucket/data/chunks/ \
  s3://my-bucket/output/tables/ \
  Record
```

### Azure HDInsight

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 10 \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=200 \
  --packages com.databricks:spark-xml_2.12:0.17.0 \
  wasbs:///jars/xml2hive.jar \
  wasbs:///data/chunks/ \
  wasbs:///output/tables/ \
  Record
```

### Google Dataproc

```bash
gcloud dataproc jobs submit spark \
  --cluster=my-cluster \
  --region=us-central1 \
  --class=XmlToParquetPipeline \
  --jars=gs://my-bucket/jars/xml2hive.jar \
  --packages=com.databricks:spark-xml_2.12:0.17.0 \
  --properties=spark.executor.memory=8g,spark.executor.cores=4,spark.sql.shuffle.partitions=200 \
  -- \
  gs://my-bucket/data/chunks/ \
  gs://my-bucket/output/tables/ \
  Record
```

---

## Performance Tuning Rules of Thumb

### Memory
```
Executor Memory ≈ (Input Data Size / Num Executors) × 2-3
For 30GB with 10 executors: (30GB / 10) × 2.5 ≈ 7.5GB → use 8GB
```

### Shuffle Partitions
```
Shuffle Partitions = Total Cores × 2-3
With 10 executors × 4 cores = 40 cores → 80-120 partitions → use 100-200
```

### File Size
```
Target Parquet File Size = 128-512MB
Too small: NameNode pressure, slow reads
Too large: Poor parallelism
```

### Executors vs Cores
```
Prefer MORE executors with FEWER cores
✅ 10 executors × 4 cores = 40 total
❌ 5 executors × 8 cores = 40 total (worse)
Reason: Better memory distribution, less GC pressure
```

---

## Monitoring & Validation

### Check Configuration at Runtime

```scala
// In Spark application
println(s"Executor memory: ${spark.conf.get("spark.executor.memory")}")
println(s"Shuffle partitions: ${spark.conf.get("spark.sql.shuffle.partitions")}")
println(s"Total cores: ${spark.sparkContext.defaultParallelism}")
```

### Monitor Spark UI

Key metrics to watch:
- **Task duration distribution** - should be uniform
- **GC time %** - should be < 10%
- **Shuffle read/write** - check for data skew
- **Failed tasks** - should be 0
- **Executor memory usage** - should not hit limit

### Optimization Checklist

After first run, check:
- [ ] Tasks are evenly distributed (no stragglers)
- [ ] GC time < 10% of task time
- [ ] No spill to disk (or minimal)
- [ ] Output files are 128-512MB each
- [ ] No executor OOM errors
- [ ] Network shuffle time reasonable

If any checks fail, adjust configuration and rerun.

---

## Quick Reference Card

```bash
# For 30GB XML on YARN
--executor-memory 8G
--executor-cores 4
--num-executors 10
--driver-memory 4G
--conf spark.sql.shuffle.partitions=200
--conf spark.sql.adaptive.enabled=true
--conf spark.dynamicAllocation.enabled=true

# Expected runtime: 30-60 minutes
# Expected output: ~10-15GB Parquet (compressed)
```
