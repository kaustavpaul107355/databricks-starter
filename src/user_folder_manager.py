"""
User Folder Manager

Utility functions for managing files and assets in your Databricks user folder.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
import logging

logger = logging.getLogger(__name__)

class UserFolderManager:
    """Manages operations in the user's Databricks folder."""
    
    def __init__(self, spark: SparkSession, username: str = None):
        """
        Initialize the UserFolderManager.
        
        Args:
            spark: SparkSession instance
            username: Username for the user folder (defaults to environment variable)
        """
        self.spark = spark
        self.username = username or os.environ.get('DATABRICKS_USER', 'kaustav.paul@databricks.com')
        self.user_folder = f"/Users/{self.username}"
        
        logger.info(f"Initialized UserFolderManager for: {self.user_folder}")
    
    def list_files(self, subfolder: str = "") -> DataFrame:
        """
        List files in the user folder or subfolder.
        
        Args:
            subfolder: Optional subfolder path
            
        Returns:
            DataFrame with file information
        """
        try:
            path = f"{self.user_folder}/{subfolder}".rstrip('/')
            files_df = self.spark.read.format("binaryFile").load(path)
            
            logger.info(f"Found {files_df.count()} files in {path}")
            return files_df
            
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {e}")
            raise
    
    def create_test_data(self, filename: str, data: list, schema: list) -> str:
        """
        Create a test data file in the user folder.
        
        Args:
            filename: Name of the file to create
            data: List of data rows
            schema: List of column names
            
        Returns:
            Path to the created file
        """
        try:
            # Create DataFrame
            df = self.spark.createDataFrame(data, schema)
            
            # Add audit columns
            df = df.withColumn("created_at", current_timestamp())
            df = df.withColumn("created_by", lit(self.username))
            
            # Write to user folder
            file_path = f"{self.user_folder}/{filename}"
            df.write.mode("overwrite").parquet(file_path)
            
            logger.info(f"Successfully created test data at: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to create test data: {e}")
            raise
    
    def read_file(self, filepath: str, format_type: str = "parquet") -> DataFrame:
        """
        Read a file from the user folder.
        
        Args:
            filepath: Path to the file (relative to user folder)
            format_type: File format (parquet, csv, json, delta)
            
        Returns:
            DataFrame with the file contents
        """
        try:
            full_path = f"{self.user_folder}/{filepath}"
            df = self.spark.read.format(format_type).load(full_path)
            
            logger.info(f"Successfully read file: {full_path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read file {filepath}: {e}")
            raise
    
    def write_file(self, df: DataFrame, filepath: str, format_type: str = "parquet", mode: str = "overwrite") -> str:
        """
        Write a DataFrame to a file in the user folder.
        
        Args:
            df: DataFrame to write
            filepath: Path to the file (relative to user folder)
            format_type: File format (parquet, csv, json, delta)
            mode: Write mode (overwrite, append, ignore, error)
            
        Returns:
            Path to the written file
        """
        try:
            full_path = f"{self.user_folder}/{filepath}"
            df.write.mode(mode).format(format_type).save(full_path)
            
            logger.info(f"Successfully wrote file to: {full_path}")
            return full_path
            
        except Exception as e:
            logger.error(f"Failed to write file {filepath}: {e}")
            raise
    
    def delete_file(self, filepath: str) -> bool:
        """
        Delete a file from the user folder.
        
        Args:
            filepath: Path to the file (relative to user folder)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            full_path = f"{self.user_folder}/{filepath}"
            
            # Use Spark's file system operations
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            
            path = self.spark._jvm.org.apache.hadoop.fs.Path(full_path)
            if fs.exists(path):
                fs.delete(path, True)  # True for recursive deletion
                logger.info(f"Successfully deleted: {full_path}")
                return True
            else:
                logger.warning(f"File does not exist: {full_path}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete file {filepath}: {e}")
            return False
    
    def get_folder_info(self) -> dict:
        """
        Get information about the user folder.
        
        Returns:
            Dictionary with folder information
        """
        try:
            # List all files
            files_df = self.list_files()
            
            # Get file counts by type
            file_info = {
                "user_folder": self.user_folder,
                "username": self.username,
                "total_files": files_df.count(),
                "file_types": {}
            }
            
            # Count files by extension
            if files_df.count() > 0:
                files_df = files_df.withColumn("extension", 
                    col("path").substr(col("path").rlike("\\."), -1))
                extension_counts = files_df.groupBy("extension").count().collect()
                
                for row in extension_counts:
                    file_info["file_types"][row["extension"]] = row["count"]
            
            logger.info(f"Retrieved folder info for: {self.user_folder}")
            return file_info
            
        except Exception as e:
            logger.error(f"Failed to get folder info: {e}")
            raise

def create_user_folder_manager(spark: SparkSession, username: str = None) -> UserFolderManager:
    """
    Factory function to create a UserFolderManager instance.
    
    Args:
        spark: SparkSession instance
        username: Optional username override
        
    Returns:
        UserFolderManager instance
    """
    return UserFolderManager(spark, username)
