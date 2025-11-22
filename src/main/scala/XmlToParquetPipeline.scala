import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Spark XML Ingestion Pipeline
 * 
 * Processes chunked XML files in parallel, flattens nested structures,
 * and outputs normalized relational tables as Parquet.
 * 
 * This job is designed to:
 * - Process XML chunks without OOM errors
 * - Flatten nested/repeated elements using explode
 * - Generate surrogate keys and audit columns
 * - Write normalized Parquet tables for downstream consumption
 * 
 * Usage:
 *   spark-submit --class XmlToParquetPipeline \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --executor-memory 8G \
 *     --executor-cores 4 \
 *     --driver-memory 4G \
 *     --conf spark.sql.shuffle.partitions=400 \
 *     --conf spark.memory.fraction=0.8 \
 *     --conf spark.memory.storageFraction=0.3 \
 *     --packages com.databricks:spark-xml_2.12:0.17.0 \
 *     xml2hive.jar \
 *     hdfs:///data/xml/chunks \
 *     hdfs:///data/output/tables \
 *     Record
 */
object XmlToParquetPipeline {

  case class PipelineConfig(
    inputPath: String,
    outputPath: String,
    rowTag: String,
    repartitionSize: Int = 200
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      Console.err.println("Usage: XmlToParquetPipeline <inputPath> <outputPath> <rowTag> [repartitionSize]")
      Console.err.println("Example: XmlToParquetPipeline hdfs:///data/chunks hdfs:///data/tables Record 200")
      System.exit(1)
    }

    val config = PipelineConfig(
      inputPath = args(0),
      outputPath = args(1),
      rowTag = args(2),
      repartitionSize = if (args.length > 3) args(3).toInt else 200
    )

    // Initialize Spark with optimized settings
    val spark = SparkSession.builder()
      .appName("XML to Parquet Pipeline")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.files.maxPartitionBytes", "134217728") // 128MB
      .config("spark.sql.broadcastTimeout", "600")
      .getOrCreate()

    try {
      println(s"[Pipeline] Starting XML ingestion pipeline")
      println(s"[Pipeline] Input: ${config.inputPath}")
      println(s"[Pipeline] Output: ${config.outputPath}")
      println(s"[Pipeline] Row tag: ${config.rowTag}")
      println()

      val startTime = System.currentTimeMillis()

      // Run the pipeline
      runPipeline(spark, config)

      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"\n[Pipeline] ✓ Pipeline completed successfully in ${duration}s")

    } catch {
      case e: Exception =>
        Console.err.println(s"\n[Pipeline] ✗ Pipeline FAILED: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def runPipeline(spark: SparkSession, config: PipelineConfig): Unit = {
    import spark.implicits._

    println(s"[Pipeline] Step 1: Reading XML chunks from ${config.inputPath}")
    
    // Read all XML chunk files
    // CRITICAL: We use rowTag to parse individual records, not entire file
    val rawDf = spark.read
      .format("xml")
      .option("rowTag", config.rowTag)
      .option("excludeAttribute", "false") // Keep XML attributes
      .option("treatEmptyValuesAsNulls", "true")
      .option("mode", "PERMISSIVE") // Continue on malformed records
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .load(config.inputPath)

    println(s"[Pipeline] Raw schema:")
    rawDf.printSchema()
    
    val recordCount = rawDf.count()
    println(s"[Pipeline] Total records loaded: $recordCount")

    // Add audit columns
    println(s"[Pipeline] Step 2: Adding audit columns")
    val dfWithAudit = rawDf
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
      .withColumn("record_id", monotonically_increasing_id())

    // Example: Flatten and normalize into multiple tables
    // This is a TEMPLATE - adjust based on your actual XML schema
    
    println(s"[Pipeline] Step 3: Creating normalized tables")
    
    // Example 1: Main/Parent table
    // Assumes flat fields at root level
    extractMainTable(spark, dfWithAudit, config)

    // Example 2: Child table with 1:N relationship
    // Assumes there's a nested array/collection
    extractChildTables(spark, dfWithAudit, config)

    println(s"[Pipeline] All tables written to ${config.outputPath}")
  }

  /**
   * Extract main/parent table with flat fields
   */
  def extractMainTable(spark: SparkSession, df: DataFrame, config: PipelineConfig): Unit = {
    println(s"[Pipeline]   - Extracting main table...")
    
    // Select flat fields (adjust to your schema)
    // This is a template - you'll need to customize based on actual XML structure
    val mainTable = df.select(
      col("record_id").as("id"),
      col("ingestion_timestamp"),
      col("source_file"),
      // Add your actual fields here, e.g.:
      // col("_CustomerID").as("customer_id"),
      // col("_Name").as("name"),
      // col("_Email").as("email"),
      // col("Status._VALUE").as("status"),
      // coalesce(col("OptionalField._VALUE"), lit(null)).as("optional_field")
      col("*") // TEMPORARY - replace with specific columns
    )

    // Write to Parquet with partitioning
    val outputPath = s"${config.outputPath}/main_table"
    mainTable
      .repartition(config.repartitionSize) // Avoid too many small files
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(outputPath)

    val count = mainTable.count()
    println(s"[Pipeline]   ✓ Main table: $count records -> $outputPath")
  }

  /**
   * Extract child tables from nested/repeated elements
   */
  def extractChildTables(spark: SparkSession, df: DataFrame, config: PipelineConfig): Unit = {
    // Example: Extract nested array elements
    // Check if there's a nested collection in your XML
    
    val schema = df.schema
    val nestedArrayFields = schema.fields.filter { field =>
      field.dataType match {
        case ArrayType(_, _) => true
        case _ => false
      }
    }

    if (nestedArrayFields.isEmpty) {
      println(s"[Pipeline]   - No nested arrays found, skipping child tables")
      return
    }

    nestedArrayFields.foreach { field =>
      println(s"[Pipeline]   - Extracting child table for: ${field.name}")
      
      try {
        val childTable = df
          .select(
            col("record_id").as("parent_id"),
            col("ingestion_timestamp"),
            explode_outer(col(field.name)).as("item")
          )
          .select(
            col("parent_id"),
            col("ingestion_timestamp"),
            col("item.*") // Flatten nested struct
          )
          .withColumn("child_id", monotonically_increasing_id())

        val outputPath = s"${config.outputPath}/${field.name}_table"
        childTable
          .repartition(config.repartitionSize / 2)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(outputPath)

        val count = childTable.count()
        println(s"[Pipeline]   ✓ Child table '${field.name}': $count records -> $outputPath")
        
      } catch {
        case e: Exception =>
          Console.err.println(s"[Pipeline]   ✗ Failed to extract ${field.name}: ${e.getMessage}")
      }
    }
  }

  /**
   * Advanced flattening utilities
   */
  object FlatteningUtils {
    
    /**
     * Flatten all nested structs in a DataFrame
     */
    def flattenSchema(df: DataFrame, delimiter: String = "_"): DataFrame = {
      val fields = df.schema.fields
      val fieldNames = fields.map(_.name)
      
      val dfFlattened = fields.foldLeft(df) { (accDf, field) =>
        field.dataType match {
          case struct: StructType =>
            val childFields = struct.fields.map { childField =>
              col(s"${field.name}.${childField.name}").as(s"${field.name}${delimiter}${childField.name}")
            }
            accDf.select(
              fieldNames.filter(_ != field.name).map(col) ++ childFields: _*
            )
          case _ => accDf
        }
      }
      
      // Recursively flatten if there are still nested structs
      val hasNested = dfFlattened.schema.fields.exists(_.dataType.isInstanceOf[StructType])
      if (hasNested) flattenSchema(dfFlattened, delimiter) else dfFlattened
    }

    /**
     * Extract XML attributes (usually prefixed with _)
     */
    def extractAttributes(df: DataFrame): DataFrame = {
      val attrFields = df.schema.fields.filter(_.name.startsWith("_"))
      val nonAttrFields = df.schema.fields.filterNot(_.name.startsWith("_"))
      
      df.select(
        (attrFields.map(f => col(f.name).as(f.name.substring(1))) ++
         nonAttrFields.map(f => col(f.name))): _*
      )
    }
  }
}
