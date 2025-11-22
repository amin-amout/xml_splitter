"""
PySpark XML Ingestion Pipeline

Processes chunked XML files in parallel, flattens nested structures,
and outputs normalized relational tables as Parquet.

This job is designed to:
- Process XML chunks without OOM errors
- Flatten nested/repeated elements using explode
- Generate surrogate keys and audit columns
- Write normalized Parquet tables for downstream consumption

Usage:
  spark-submit --master yarn \
    --deploy-mode cluster \
    --executor-memory 8G \
    --executor-cores 4 \
    --driver-memory 4G \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.memory.fraction=0.8 \
    --conf spark.memory.storageFraction=0.3 \
    --packages com.databricks:spark-xml_2.12:0.17.0 \
    xml_to_parquet_pipeline.py \
    hdfs:///data/xml/chunks \
    hdfs:///data/output/tables \
    Record
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    monotonically_increasing_id, explode_outer, 
    coalesce, lit, when
)
from pyspark.sql.types import StructType, ArrayType
from datetime import datetime


class XmlToParquetPipeline:
    """XML to Parquet ingestion pipeline"""
    
    def __init__(self, input_path, output_path, row_tag, repartition_size=200):
        self.input_path = input_path
        self.output_path = output_path
        self.row_tag = row_tag
        self.repartition_size = repartition_size
        self.spark = None
    
    def init_spark(self):
        """Initialize Spark session with optimized settings"""
        self.spark = SparkSession.builder \
            .appName("XML to Parquet Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.sql.broadcastTimeout", "600") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"[Pipeline] Spark initialized - Version: {self.spark.version}")
        print(f"[Pipeline] Executor memory: {self.spark.conf.get('spark.executor.memory', 'default')}")
        print(f"[Pipeline] Shuffle partitions: {self.spark.conf.get('spark.sql.shuffle.partitions', 'default')}")
    
    def run(self):
        """Execute the complete pipeline"""
        try:
            print(f"[Pipeline] Starting XML ingestion pipeline")
            print(f"[Pipeline] Input: {self.input_path}")
            print(f"[Pipeline] Output: {self.output_path}")
            print(f"[Pipeline] Row tag: {self.row_tag}")
            print()
            
            start_time = datetime.now()
            
            # Load XML data
            raw_df = self._load_xml()
            
            # Add audit columns
            df_with_audit = self._add_audit_columns(raw_df)
            
            # Extract and write tables
            self._extract_tables(df_with_audit)
            
            duration = (datetime.now() - start_time).total_seconds()
            print(f"\n[Pipeline] ✓ Pipeline completed successfully in {duration:.1f}s")
            
        except Exception as e:
            print(f"\n[Pipeline] ✗ Pipeline FAILED: {str(e)}", file=sys.stderr)
            raise
        finally:
            if self.spark:
                self.spark.stop()
    
    def _load_xml(self):
        """Load XML chunks using spark-xml"""
        print(f"[Pipeline] Step 1: Reading XML chunks from {self.input_path}")
        
        # Read all XML chunk files
        # CRITICAL: We use rowTag to parse individual records, not entire file
        raw_df = self.spark.read \
            .format("xml") \
            .option("rowTag", self.row_tag) \
            .option("excludeAttribute", "false") \
            .option("treatEmptyValuesAsNulls", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(self.input_path)
        
        print(f"[Pipeline] Raw schema:")
        raw_df.printSchema()
        
        record_count = raw_df.count()
        print(f"[Pipeline] Total records loaded: {record_count}")
        
        # Check for corrupt records
        if "_corrupt_record" in raw_df.columns:
            corrupt_count = raw_df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                print(f"[Pipeline] ⚠ Warning: {corrupt_count} corrupt records detected")
        
        return raw_df
    
    def _add_audit_columns(self, df):
        """Add ingestion metadata columns"""
        print(f"[Pipeline] Step 2: Adding audit columns")
        
        return df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name()) \
            .withColumn("record_id", monotonically_increasing_id())
    
    def _extract_tables(self, df):
        """Extract and write normalized tables"""
        print(f"[Pipeline] Step 3: Creating normalized tables")
        
        # Extract main table
        self._extract_main_table(df)
        
        # Extract child tables from nested arrays
        self._extract_child_tables(df)
        
        print(f"[Pipeline] All tables written to {self.output_path}")
    
    def _extract_main_table(self, df):
        """Extract main/parent table with flat fields"""
        print(f"[Pipeline]   - Extracting main table...")
        
        # Get all column names
        columns = df.columns
        
        # Identify nested structs and arrays for exclusion from main table
        nested_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                if not field.name.startswith("_"):  # Keep attributes
                    nested_cols.append(field.name)
        
        # Select flat fields (exclude nested collections)
        flat_cols = [c for c in columns if c not in nested_cols]
        
        main_table = df.select(*flat_cols)
        
        # Optional: Flatten nested structs in main table
        main_table = self._flatten_structs(main_table)
        
        # Write to Parquet
        output_path = f"{self.output_path}/main_table"
        main_table \
            .repartition(self.repartition_size) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        count = main_table.count()
        print(f"[Pipeline]   ✓ Main table: {count} records -> {output_path}")
    
    def _extract_child_tables(self, df):
        """Extract child tables from nested/repeated elements"""
        # Find array fields (repeated elements)
        array_fields = [
            field for field in df.schema.fields
            if isinstance(field.dataType, ArrayType)
        ]
        
        if not array_fields:
            print(f"[Pipeline]   - No nested arrays found, skipping child tables")
            return
        
        for field in array_fields:
            print(f"[Pipeline]   - Extracting child table for: {field.name}")
            
            try:
                # Explode array and create child table
                child_table = df.select(
                    col("record_id").alias("parent_id"),
                    col("ingestion_timestamp"),
                    explode_outer(col(field.name)).alias("item")
                )
                
                # If item is a struct, flatten it
                if isinstance(field.dataType.elementType, StructType):
                    child_table = child_table.select(
                        "parent_id",
                        "ingestion_timestamp",
                        "item.*"
                    )
                else:
                    # Simple array of primitives
                    child_table = child_table.withColumn("value", col("item"))
                
                # Add child-specific ID
                child_table = child_table.withColumn(
                    "child_id", 
                    monotonically_increasing_id()
                )
                
                # Write to Parquet
                output_path = f"{self.output_path}/{field.name}_table"
                child_table \
                    .repartition(self.repartition_size // 2) \
                    .write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(output_path)
                
                count = child_table.count()
                print(f"[Pipeline]   ✓ Child table '{field.name}': {count} records -> {output_path}")
                
            except Exception as e:
                print(f"[Pipeline]   ✗ Failed to extract {field.name}: {str(e)}", file=sys.stderr)
    
    def _flatten_structs(self, df, delimiter="_"):
        """Recursively flatten nested struct columns"""
        # Get struct fields
        struct_fields = [
            field for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]
        
        if not struct_fields:
            return df
        
        # Flatten one level
        select_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                # Expand struct fields
                for sub_field in field.dataType.fields:
                    select_cols.append(
                        col(f"{field.name}.{sub_field.name}")
                        .alias(f"{field.name}{delimiter}{sub_field.name}")
                    )
            else:
                select_cols.append(col(field.name))
        
        flattened_df = df.select(*select_cols)
        
        # Recursively flatten if more structs exist
        if any(isinstance(f.dataType, StructType) for f in flattened_df.schema.fields):
            return self._flatten_structs(flattened_df, delimiter)
        
        return flattened_df


def main():
    """Main entry point"""
    if len(sys.argv) < 4:
        print("Usage: xml_to_parquet_pipeline.py <inputPath> <outputPath> <rowTag> [repartitionSize]", 
              file=sys.stderr)
        print("Example: xml_to_parquet_pipeline.py hdfs:///data/chunks hdfs:///data/tables Record 200",
              file=sys.stderr)
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    row_tag = sys.argv[3]
    repartition_size = int(sys.argv[4]) if len(sys.argv) > 4 else 200
    
    # Create and run pipeline
    pipeline = XmlToParquetPipeline(input_path, output_path, row_tag, repartition_size)
    pipeline.init_spark()
    pipeline.run()


if __name__ == "__main__":
    main()
