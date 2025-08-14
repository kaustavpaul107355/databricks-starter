"""
Tests for Databricks utilities.
"""

import pytest
from src.databricks_utils import validate_dataframe, add_audit_columns
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TestApp") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("role", StringType(), True)
    ])
    
    data = [
        ("Alice", 25, "Engineer"),
        ("Bob", 30, "Data Scientist"),
        ("Charlie", 35, "Manager")
    ]
    
    return spark.createDataFrame(data, schema)

def test_validate_dataframe_success(sample_df):
    """Test DataFrame validation with valid data."""
    required_columns = ["name", "age", "role"]
    result = validate_dataframe(sample_df, required_columns)
    assert result is True

def test_validate_dataframe_missing_columns(sample_df):
    """Test DataFrame validation with missing columns."""
    required_columns = ["name", "age", "role", "missing_column"]
    result = validate_dataframe(sample_df, required_columns)
    assert result is False

def test_validate_dataframe_no_required_columns(sample_df):
    """Test DataFrame validation without required columns."""
    result = validate_dataframe(sample_df)
    assert result is True

def test_add_audit_columns(sample_df):
    """Test adding audit columns to DataFrame."""
    result_df = add_audit_columns(sample_df, "test_user")
    
    # Check that audit columns were added
    expected_columns = ["name", "age", "role", "created_at", "created_by", "updated_at", "updated_by"]
    assert result_df.columns == expected_columns
    
    # Check that created_by and updated_by have correct values
    created_by_values = result_df.select("created_by").distinct().collect()
    assert len(created_by_values) == 1
    assert created_by_values[0]["created_by"] == "test_user"

if __name__ == "__main__":
    pytest.main([__file__])
