"""
Databricks Utilities

Common utility functions for working with Databricks.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """
    Create or get a Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession instance
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        logger.info(f"Spark session '{app_name}' created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def read_delta_table(spark: SparkSession, table_name: str, catalog: str = None, schema: str = None) -> DataFrame:
    """
    Read a Delta table.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the table to read
        catalog: Catalog name (optional)
        schema: Schema name (optional)
        
    Returns:
        DataFrame containing the table data
    """
    try:
        if catalog and schema:
            full_table_name = f"{catalog}.{schema}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        df = spark.read.table(full_table_name)
        logger.info(f"Successfully read table: {full_table_name}")
        return df
    except Exception as e:
        logger.error(f"Failed to read table {table_name}: {e}")
        raise

def write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite", 
                     catalog: str = None, schema: str = None) -> None:
    """
    Write a DataFrame to a Delta table.
    
    Args:
        df: DataFrame to write
        table_name: Name of the target table
        mode: Write mode (overwrite, append, ignore, error)
        catalog: Catalog name (optional)
        schema: Schema name (optional)
    """
    try:
        if catalog and schema:
            full_table_name = f"{catalog}.{schema}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        df.write.mode(mode).saveAsTable(full_table_name)
        logger.info(f"Successfully wrote to table: {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to write to table {table_name}: {e}")
        raise

def add_audit_columns(df: DataFrame, user: str = "system") -> DataFrame:
    """
    Add audit columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        user: User performing the operation
        
    Returns:
        DataFrame with audit columns added
    """
    from pyspark.sql.functions import current_timestamp
    
    return df.withColumn("created_at", current_timestamp()) \
             .withColumn("created_by", lit(user)) \
             .withColumn("updated_at", current_timestamp()) \
             .withColumn("updated_by", lit(user))

def validate_dataframe(df: DataFrame, required_columns: list = None) -> bool:
    """
    Validate a DataFrame for required columns and data quality.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if validation passes, False otherwise
    """
    try:
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
        
        # Check for null values in key columns
        if required_columns:
            for col_name in required_columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    logger.warning(f"Column {col_name} has {null_count} null values")
        
        logger.info("DataFrame validation passed")
        return True
        
    except Exception as e:
        logger.error(f"DataFrame validation failed: {e}")
        return False

def get_table_info(spark: SparkSession, table_name: str, catalog: str = None, schema: str = None) -> dict:
    """
    Get information about a table.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the table
        catalog: Catalog name (optional)
        schema: Schema name (optional)
        
    Returns:
        Dictionary containing table information
    """
    try:
        if catalog and schema:
            full_table_name = f"{catalog}.{schema}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        df = spark.read.table(full_table_name)
        
        info = {
            "table_name": full_table_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "schema": df.schema.json()
        }
        
        logger.info(f"Retrieved table info for: {full_table_name}")
        return info
        
    except Exception as e:
        logger.error(f"Failed to get table info for {table_name}: {e}")
        raise
