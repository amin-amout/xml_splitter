import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import play.api.libs.json._

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
    mappingPath: String,
    repartitionSize: Int = 200
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      Console.err.println("Usage: XmlToParquetPipeline <inputPath> <outputPath> <rowTag> <mappingPath> [repartitionSize]")
      Console.err.println("Example: XmlToParquetPipeline hdfs:///data/chunks hdfs:///data/tables Order conf/mapping.json 200")
      System.exit(1)
    }

    val config = PipelineConfig(
      inputPath = args(0),
      outputPath = args(1),
      rowTag = args(2),
      mappingPath = args(3),
      repartitionSize = if (args.length > 4) args(4).toInt else 200
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

    val rawDf = spark.read
      .format("xml")
      .option("rowTag", config.rowTag)
      .option("excludeAttribute", "false")
      .option("treatEmptyValuesAsNulls", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .load(config.inputPath)

    println(s"[Pipeline] Raw schema:")
    rawDf.printSchema()

    val recordCount = rawDf.count()
    println(s"[Pipeline] Total records loaded: $recordCount")

    println(s"[Pipeline] Step 2: Adding audit columns")
    val dfWithAudit = rawDf
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
      .withColumn("record_id", monotonically_increasing_id())

    // Load mapping.json
    val mappingSource = Source.fromFile(config.mappingPath)
    val mappingJson = try mappingSource.mkString finally mappingSource.close()
    val json = Json.parse(mappingJson)

    // For each table in mapping.json
    json.as[JsObject].fields.foreach { case (tableName, columnsArr) =>
      columnsArr match {
        case arr: JsArray =>
          val columns = arr.value.map { colMap =>
            val colName = (colMap \ "column").as[String]
            val source = (colMap \ "source").as[String]
            (colName, source)
          }

          // Determine if we need to explode an array (child table)
          val explodeInfo = columns.collectFirst {
            case (colName, source) if source.contains("OrderItems.Item") => ("OrderItems.Item", "item")
            case (colName, source) if source.contains("Tags.Tag") => ("Tags.Tag", "tag")
          }

          val tableDf = explodeInfo match {
            case Some((arrayPath, alias)) =>
              // Explode array for child table
              val exploded = dfWithAudit.withColumn(alias, explode_outer(col(arrayPath)))
              val selectCols = columns.map {
                case (colName, source) if source.startsWith(arrayPath + ".") =>
                  // Select from exploded struct
                  col(s"$alias.${source.stripPrefix(arrayPath + ".")}").as(colName)
                case (colName, source) if source == arrayPath =>
                  col(alias).as(colName)
                case (colName, source) =>
                  col(source).as(colName)
              }
              exploded.select(selectCols: _*)
            case None =>
              // Main table (no explode)
              val selectCols = columns.map { case (colName, source) => col(source).as(colName) }
              dfWithAudit.select(selectCols: _*)
          }

          println(s"[DEBUG] Table: $tableName schema:")
          tableDf.printSchema()
          println(s"[DEBUG] Table: $tableName sample rows:")
          tableDf.show(10, truncate = false)
          val count = tableDf.count()
          println(s"[DEBUG] Table: $tableName row count: $count")

          val outputPath = s"${config.outputPath}/$tableName"
          tableDf
            .repartition(config.repartitionSize)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(outputPath)
          println(s"[Pipeline]   ✓ $tableName: $count records -> $outputPath")
        case other =>
          throw new RuntimeException(s"Table mapping for '$tableName' is not an array: " + Json.prettyPrint(other))
      }
    }
  }

  /**
   * Extract main/parent table with flat fields
   */
  def extractMainTable(spark: SparkSession, df: DataFrame, config: PipelineConfig): Unit = {
    println(s"[Pipeline]   - Extracting main table (using mapping file: ${config.mappingPath})...")
    val mappingSource = Source.fromFile(config.mappingPath)
    val mappingJson = try mappingSource.mkString finally mappingSource.close()
    val json = Json.parse(mappingJson)
    val mainTableMapping = (json \ "main_table").head.as[JsArray].value
    val baseColumns: Seq[Column] = mainTableMapping.map { colMap =>
      val colName = (colMap \ "column").head.as[String]
      val source = (colMap \ "source").head.as[String]
      col(source).as(colName)
    }
    val columns =
      if (baseColumns.exists(_.toString.contains("order_id"))) baseColumns
      else baseColumns :+ col("_OrderID").as("order_id")
    val mainTable = df.select(columns: _*)
    println("[DEBUG] Main table schema:")
    mainTable.printSchema()
    println("[DEBUG] Main table sample rows:")
    mainTable.show(10, truncate = false)
    val count = mainTable.count()
    println(s"[DEBUG] Main table row count: $count")
    val outputPath = s"${config.outputPath}/main_table"
    mainTable
      .repartition(config.repartitionSize)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(outputPath)
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
          .withColumn("order_id", col("_OrderID")) // Always add order_id for business-level linking

        println(s"[DEBUG] Child table '${field.name}' schema:")
        childTable.printSchema()
        println(s"[DEBUG] Child table '${field.name}' sample rows:")
        childTable.show(10, truncate = false)
        val count = childTable.count()
        println(s"[DEBUG] Child table '${field.name}' row count: $count")

        val outputPath = s"${config.outputPath}/${field.name}_table"
        childTable
          .repartition(config.repartitionSize / 2)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(outputPath)

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
