#!/usr/bin/env python3
"""
Adventure Works Data Warehouse - Databricks DDL Script (Jupyter Compatible)

This script creates the Adventure Works Data Warehouse schema in Databricks using Delta Lake format.
Compatible with Jupyter Notebook + Databricks Connect (no magic commands).

UPDATED FEATURES:
- Target Catalog.Schema: kaustavpaul_demo.adventureworks
- Volume Integration: /Volumes/kaustavpaul_demo/adventureworks/dwh/
- Complete DDL: All dimension and fact tables from Adventure Works DW
- Data Loading: Automated loading from Databricks Volume
- Full Pipeline: Create tables → Load data → Verify

USAGE:
1. Basic Setup (Tables Only):    python AdventureWorks_DW_DDL_JupyterReady.py
2. Complete Setup (With Data):   python -c "from AdventureWorks_DW_DDL_JupyterReady import run_complete_setup; run_complete_setup()"
3. Interactive:                  Import and use individual functions

Created for: Databricks Runtime (via Databricks Connect)
Data Format: Delta Lake
Source: Microsoft Adventure Works Data Warehouse Sample Database
Volume: /Volumes/kaustavpaul_demo/adventureworks/dwh/
Target: kaustavpaul_demo.adventureworks catalog.schema
"""

import os
from databricks.connect import DatabricksSession

def setup_databricks_connection():
    """Initialize Databricks Connect session"""
    print("🔧 Setting up Databricks Connect...")
    
    # Configure Databricks Connect
    os.environ['DATABRICKS_CONFIG_PROFILE'] = 'DEFAULT'
    os.environ['DATABRICKS_CLUSTER_ID'] = '0312-222653-nqfcg6yd'  # Kaustav Paul's ML Compute
    
    # Create Spark session connected to your cluster
    spark = DatabricksSession.builder.getOrCreate()
    
    print(f"✅ Connected to Databricks cluster!")
    print(f"Spark version: {spark.version}")
    print(f"Cluster ID: 0312-222653-nqfcg6yd (Kaustav Paul's ML Compute)")
    
    return spark

def create_database(spark, catalog_schema="kaustavpaul_demo.adventureworks"):
    """Create and use the Adventure Works database"""
    print(f"\n📚 Creating catalog.schema: {catalog_schema}")
    
    # Create catalog and schema if they don't exist
    catalog, schema = catalog_schema.split('.')
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")
    spark.sql(f"USE {catalog_schema}")
    
    print(f"✅ Using catalog.schema: {catalog_schema}")

def create_dimension_tables(spark):
    """Create all dimension tables"""
    print("\n🏗️  Creating Dimension Tables...")
    
    # DimDate table
    print("Creating DimDate...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimDate (
        DateKey INT NOT NULL,
        FullDateAlternateKey DATE NOT NULL,
        DayNumberOfWeek TINYINT NOT NULL,
        EnglishDayNameOfWeek STRING NOT NULL,
        SpanishDayNameOfWeek STRING NOT NULL,
        FrenchDayNameOfWeek STRING NOT NULL,
        DayNumberOfMonth TINYINT NOT NULL,
        DayNumberOfYear SMALLINT NOT NULL,
        WeekNumberOfYear TINYINT NOT NULL,
        EnglishMonthName STRING NOT NULL,
        SpanishMonthName STRING NOT NULL,
        FrenchMonthName STRING NOT NULL,
        MonthNumberOfYear TINYINT NOT NULL,
        CalendarQuarter TINYINT NOT NULL,
        CalendarYear SMALLINT NOT NULL,
        CalendarSemester TINYINT NOT NULL,
        FiscalQuarter TINYINT NOT NULL,
        FiscalYear SMALLINT NOT NULL,
        FiscalSemester TINYINT NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimGeography table
    print("Creating DimGeography...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimGeography (
        GeographyKey INT NOT NULL,
        City STRING,
        StateProvinceCode STRING,
        StateProvinceName STRING,
        CountryRegionCode STRING,
        EnglishCountryRegionName STRING,
        SpanishCountryRegionName STRING,
        FrenchCountryRegionName STRING,
        PostalCode STRING,
        SalesTerritoryKey INT
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimCustomer table
    print("Creating DimCustomer...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimCustomer (
        CustomerKey INT NOT NULL,
        GeographyKey INT,
        CustomerAlternateKey STRING,
        Title STRING,
        FirstName STRING,
        MiddleName STRING,
        LastName STRING,
        NameStyle BOOLEAN,
        BirthDate DATE,
        MaritalStatus STRING,
        Suffix STRING,
        Gender STRING,
        EmailAddress STRING,
        YearlyIncome DECIMAL(19,4),
        TotalChildren TINYINT,
        NumberChildrenAtHome TINYINT,
        EnglishEducation STRING,
        SpanishEducation STRING,
        FrenchEducation STRING,
        EnglishOccupation STRING,
        SpanishOccupation STRING,
        FrenchOccupation STRING,
        HouseOwnerFlag STRING,
        NumberCarsOwned TINYINT,
        AddressLine1 STRING,
        AddressLine2 STRING,
        Phone STRING,
        DateFirstPurchase DATE,
        CommuteDistance STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimProduct table
    print("Creating DimProduct...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimProduct (
        ProductKey INT NOT NULL,
        ProductAlternateKey STRING,
        ProductSubcategoryKey INT,
        WeightUnitMeasureCode STRING,
        SizeUnitMeasureCode STRING,
        EnglishProductName STRING,
        SpanishProductName STRING,
        FrenchProductName STRING,
        StandardCost DECIMAL(19,4),
        FinishedGoodsFlag BOOLEAN,
        Color STRING,
        SafetyStockLevel SMALLINT,
        ReorderPoint SMALLINT,
        ListPrice DECIMAL(19,4),
        Size STRING,
        SizeRange STRING,
        Weight DOUBLE,
        DaysToManufacture INT,
        ProductLine STRING,
        DealerPrice DECIMAL(19,4),
        Class STRING,
        Style STRING,
        ModelName STRING,
        LargePhoto BINARY,
        EnglishDescription STRING,
        FrenchDescription STRING,
        ChineseDescription STRING,
        ArabicDescription STRING,
        HebrewDescription STRING,
        ThaiDescription STRING,
        GermanDescription STRING,
        JapaneseDescription STRING,
        TurkishDescription STRING,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP,
        Status STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimProductCategory table
    print("Creating DimProductCategory...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimProductCategory (
        ProductCategoryKey INT NOT NULL,
        ProductCategoryAlternateKey INT,
        EnglishProductCategoryName STRING,
        SpanishProductCategoryName STRING,
        FrenchProductCategoryName STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimProductSubcategory table
    print("Creating DimProductSubcategory...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimProductSubcategory (
        ProductSubcategoryKey INT NOT NULL,
        ProductSubcategoryAlternateKey INT,
        EnglishProductSubcategoryName STRING,
        SpanishProductSubcategoryName STRING,
        FrenchProductSubcategoryName STRING,
        ProductCategoryKey INT
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimEmployee table
    print("Creating DimEmployee...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimEmployee (
        EmployeeKey INT NOT NULL,
        ParentEmployeeKey INT,
        EmployeeNationalIDAlternateKey STRING,
        ParentEmployeeNationalIDAlternateKey STRING,
        SalesTerritoryKey INT,
        FirstName STRING,
        LastName STRING,
        MiddleName STRING,
        NameStyle BOOLEAN,
        Title STRING,
        HireDate DATE,
        BirthDate DATE,
        LoginID STRING,
        EmailAddress STRING,
        Phone STRING,
        MaritalStatus STRING,
        EmergencyContactName STRING,
        EmergencyContactPhone STRING,
        SalariedFlag BOOLEAN,
        Gender STRING,
        PayFrequency TINYINT,
        BaseRate DECIMAL(19,4),
        VacationHours SMALLINT,
        SickLeaveHours SMALLINT,
        CurrentFlag BOOLEAN,
        SalesPersonFlag BOOLEAN,
        DepartmentName STRING,
        StartDate DATE,
        EndDate DATE,
        Status STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimCurrency table
    print("Creating DimCurrency...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimCurrency (
        CurrencyKey INT NOT NULL,
        CurrencyAlternateKey STRING,
        CurrencyName STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimPromotion table
    print("Creating DimPromotion...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimPromotion (
        PromotionKey INT NOT NULL,
        PromotionAlternateKey INT,
        EnglishPromotionName STRING,
        SpanishPromotionName STRING,
        FrenchPromotionName STRING,
        DiscountPct DOUBLE,
        EnglishPromotionType STRING,
        SpanishPromotionType STRING,
        FrenchPromotionType STRING,
        EnglishPromotionCategory STRING,
        SpanishPromotionCategory STRING,
        FrenchPromotionCategory STRING,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP,
        MinQty INT,
        MaxQty INT
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimReseller table
    print("Creating DimReseller...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimReseller (
        ResellerKey INT NOT NULL,
        GeographyKey INT,
        ResellerAlternateKey STRING,
        Phone STRING,
        BusinessType STRING,
        ResellerName STRING,
        NumberEmployees INT,
        OrderFrequency STRING,
        OrderMonth TINYINT,
        FirstOrderYear INT,
        LastOrderYear INT,
        ProductLine STRING,
        AddressLine1 STRING,
        AddressLine2 STRING,
        AnnualSales DECIMAL(19,4),
        BankName STRING,
        MinPaymentType TINYINT,
        MinPaymentAmount DECIMAL(19,4),
        AnnualRevenue DECIMAL(19,4),
        YearOpened INT
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimSalesTerritory table
    print("Creating DimSalesTerritory...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimSalesTerritory (
        SalesTerritoryKey INT NOT NULL,
        SalesTerritoryAlternateKey INT,
        SalesTerritoryRegion STRING,
        SalesTerritoryCountry STRING,
        SalesTerritoryGroup STRING,
        SalesTerritoryImage BINARY
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # DimSalesReason table
    print("Creating DimSalesReason...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DimSalesReason (
        SalesReasonKey INT NOT NULL,
        SalesReasonAlternateKey INT,
        SalesReasonName STRING,
        SalesReasonReasonType STRING
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print("✅ All dimension tables created!")

def create_fact_tables(spark):
    """Create all fact tables"""
    print("\n📊 Creating Fact Tables...")
    
    # FactInternetSales table
    print("Creating FactInternetSales...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS FactInternetSales (
        ProductKey INT NOT NULL,
        OrderDateKey INT NOT NULL,
        DueDateKey INT NOT NULL,
        ShipDateKey INT NOT NULL,
        CustomerKey INT NOT NULL,
        PromotionKey INT NOT NULL,
        CurrencyKey INT NOT NULL,
        SalesTerritoryKey INT NOT NULL,
        SalesOrderNumber STRING NOT NULL,
        SalesOrderLineNumber TINYINT NOT NULL,
        RevisionNumber TINYINT NOT NULL,
        OrderQuantity SMALLINT NOT NULL,
        UnitPrice DECIMAL(19,4) NOT NULL,
        ExtendedAmount DECIMAL(19,4) NOT NULL,
        UnitPriceDiscountPct DOUBLE NOT NULL,
        DiscountAmount DOUBLE NOT NULL,
        ProductStandardCost DECIMAL(19,4) NOT NULL,
        TotalProductCost DECIMAL(19,4) NOT NULL,
        SalesAmount DECIMAL(19,4) NOT NULL,
        TaxAmt DECIMAL(19,4) NOT NULL,
        Freight DECIMAL(19,4) NOT NULL,
        CarrierTrackingNumber STRING,
        CustomerPONumber STRING
    )
    USING DELTA
    PARTITIONED BY (OrderDateKey)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # FactResellerSales table
    print("Creating FactResellerSales...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS FactResellerSales (
        ProductKey INT NOT NULL,
        OrderDateKey INT NOT NULL,
        DueDateKey INT NOT NULL,
        ShipDateKey INT NOT NULL,
        ResellerKey INT NOT NULL,
        EmployeeKey INT NOT NULL,
        PromotionKey INT NOT NULL,
        CurrencyKey INT NOT NULL,
        SalesTerritoryKey INT NOT NULL,
        SalesOrderNumber STRING NOT NULL,
        SalesOrderLineNumber TINYINT NOT NULL,
        RevisionNumber TINYINT NOT NULL,
        OrderQuantity SMALLINT NOT NULL,
        UnitPrice DECIMAL(19,4) NOT NULL,
        ExtendedAmount DECIMAL(19,4) NOT NULL,
        UnitPriceDiscountPct DOUBLE NOT NULL,
        DiscountAmount DOUBLE NOT NULL,
        ProductStandardCost DECIMAL(19,4) NOT NULL,
        TotalProductCost DECIMAL(19,4) NOT NULL,
        SalesAmount DECIMAL(19,4) NOT NULL,
        TaxAmt DECIMAL(19,4) NOT NULL,
        Freight DECIMAL(19,4) NOT NULL,
        CarrierTrackingNumber STRING
    )
    USING DELTA
    PARTITIONED BY (OrderDateKey)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # FactCurrencyRate table
    print("Creating FactCurrencyRate...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS FactCurrencyRate (
        CurrencyKey INT NOT NULL,
        DateKey INT NOT NULL,
        AverageRate DOUBLE,
        EndOfDayRate DOUBLE
    )
    USING DELTA
    PARTITIONED BY (DateKey)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # FactCallCenter table
    print("Creating FactCallCenter...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS FactCallCenter (
        FactCallCenterID INT NOT NULL,
        DateKey INT NOT NULL,
        WageType STRING,
        Shift STRING,
        LevelOneOperators INT,
        LevelTwoOperators INT,
        TotalOperators INT,
        Calls INT,
        AutomaticResponses INT,
        Orders INT,
        IssuesRaised INT,
        AverageTimePerIssue INT,
        ServiceGrade DOUBLE
    )
    USING DELTA
    PARTITIONED BY (DateKey)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print("✅ All fact tables created!")

def verify_tables(spark):
    """Verify that all tables have been created"""
    print("\n🔍 Verifying Tables...")
    
    # Show all tables
    tables_df = spark.sql("SHOW TABLES")
    tables = [row.tableName for row in tables_df.collect()]
    
    print(f"📊 Created {len(tables)} tables:")
    for table in sorted(tables):
        print(f"  • {table}")
    
    # Get table details for a few key tables
    print("\n📋 Table Details:")
    for table in ['DimDate', 'DimCustomer', 'FactInternetSales']:
        if table.lower() in [t.lower() for t in tables]:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0]['count']
                print(f"  • {table}: {count} rows")
            except Exception as e:
                print(f"  • {table}: Created (unable to count: {str(e)})")
    
    print("\n✅ Verification complete!")

def load_csv_to_table(spark, csv_path, table_name, header=True, infer_schema=True):
    """
    Load data from CSV file into Delta table
    """
    try:
        # Read CSV
        df = spark.read.option("header", header).option("inferSchema", infer_schema).csv(csv_path)
        
        # Write to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        
        print(f"✅ Loaded {df.count()} rows into {table_name}")
        
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {str(e)}")

def load_adventure_works_data(spark, catalog_schema="kaustavpaul_demo.adventureworks"):
    """
    Load all Adventure Works data from the Databricks volume
    """
    print("\n📥 Loading Adventure Works Data from Volume...")
    
    # Volume path where all CSV files are located
    volume_path = "/Volumes/kaustavpaul_demo/adventureworks/dwh"
    
    # Define mapping of CSV files to table names
    data_mappings = {
        # Dimension Tables
        f"{volume_path}/DimDate.csv": f"{catalog_schema}.DimDate",
        f"{volume_path}/DimGeography.csv": f"{catalog_schema}.DimGeography",
        f"{volume_path}/DimCustomer.csv": f"{catalog_schema}.DimCustomer",
        f"{volume_path}/DimProduct.csv": f"{catalog_schema}.DimProduct",
        f"{volume_path}/DimProductCategory.csv": f"{catalog_schema}.DimProductCategory",
        f"{volume_path}/DimProductSubcategory.csv": f"{catalog_schema}.DimProductSubcategory",
        f"{volume_path}/DimEmployee.csv": f"{catalog_schema}.DimEmployee",
        f"{volume_path}/DimCurrency.csv": f"{catalog_schema}.DimCurrency",
        f"{volume_path}/DimPromotion.csv": f"{catalog_schema}.DimPromotion",
        f"{volume_path}/DimReseller.csv": f"{catalog_schema}.DimReseller",
        f"{volume_path}/DimSalesTerritory.csv": f"{catalog_schema}.DimSalesTerritory",
        f"{volume_path}/DimSalesReason.csv": f"{catalog_schema}.DimSalesReason",
        # Fact Tables
        f"{volume_path}/FactCurrencyRate.csv": f"{catalog_schema}.FactCurrencyRate",
        f"{volume_path}/FactCallCenter.csv": f"{catalog_schema}.FactCallCenter",
    }
    
    # Load each file
    for csv_path, table_name in data_mappings.items():
        print(f"Loading {csv_path.split('/')[-1]} → {table_name.split('.')[-1]}...")
        load_csv_to_table(spark, csv_path, table_name)
    
    print("✅ All available data loaded!")

def load_sales_data(spark, catalog_schema="kaustavpaul_demo.adventureworks"):
    """
    Load sales fact tables (these typically require more specific handling)
    """
    print("\n📊 Loading Sales Fact Data...")
    
    volume_path = "/Volumes/kaustavpaul_demo/adventureworks/dwh"
    
    # Check if sales fact files exist and load them
    sales_files = {
        f"{volume_path}/FactInternetSales.csv": f"{catalog_schema}.FactInternetSales",
        f"{volume_path}/FactResellerSales.csv": f"{catalog_schema}.FactResellerSales",
    }
    
    for csv_path, table_name in sales_files.items():
        try:
            # Check if file exists by trying to read first few rows
            test_df = spark.read.option("header", "true").csv(csv_path).limit(1)
            if test_df.count() > 0 or True:  # File exists or we want to try anyway
                print(f"Loading {csv_path.split('/')[-1]} → {table_name.split('.')[-1]}...")
                load_csv_to_table(spark, csv_path, table_name)
        except Exception as e:
            print(f"⚠️  {csv_path.split('/')[-1]} not found or failed to load: {str(e)}")
    
    print("✅ Sales data loading complete!")

def get_volume_files(spark, volume_path="/Volumes/kaustavpaul_demo/adventureworks/dwh"):
    """
    List all files available in the volume for data loading
    """
    print(f"\n📂 Files available in volume: {volume_path}")
    
    try:
        files_df = spark.sql(f'LIST "{volume_path}"')
        files = files_df.collect()
        
        csv_files = [row.name for row in files if row.name.endswith('.csv')]
        
        print(f"📁 Found {len(csv_files)} CSV files:")
        for file in sorted(csv_files):
            print(f"  • {file}")
        
        return csv_files
        
    except Exception as e:
        print(f"❌ Failed to list volume files: {str(e)}")
        return []

def main():
    """Main execution function"""
    print("🚀 Adventure Works Data Warehouse DDL Script")
    print("=" * 50)
    
    # Setup connection
    spark = setup_databricks_connection()
    
    # Create database
    create_database(spark)
    
    # Create tables
    create_dimension_tables(spark)
    create_fact_tables(spark)
    
    # Verify
    verify_tables(spark)
    
    print("\n🎉 Adventure Works Data Warehouse Setup Complete!")
    print("=" * 50)
    print("\n📋 Next Steps:")
    print("1. Load data using: load_adventure_works_data(spark)")
    print("2. List available files: get_volume_files(spark)")
    print("3. Query data using: spark.sql('SELECT * FROM kaustavpaul_demo.adventureworks.DimCustomer LIMIT 10').show()")
    print("4. Optimize tables using: spark.sql('OPTIMIZE kaustavpaul_demo.adventureworks.FactInternetSales')")
    
    return spark

def run_complete_setup():
    """Run complete Adventure Works setup including data loading"""
    print("🚀 Running Complete Adventure Works Data Warehouse Setup")
    print("=" * 60)
    
    # Step 1: Create tables
    spark = main()
    
    # Step 2: List available files in volume
    get_volume_files(spark)
    
    # Step 3: Load data
    load_adventure_works_data(spark)
    load_sales_data(spark)
    
    # Step 4: Run some sample queries
    print("\n📊 Running Sample Queries...")
    
    try:
        # Count records in key dimension tables
        for table in ['DimDate', 'DimCustomer', 'DimProduct']:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM kaustavpaul_demo.adventureworks.{table}").collect()[0]['count']
                print(f"  • {table}: {count:,} records")
            except Exception as e:
                print(f"  • {table}: Unable to query ({str(e)})")
        
        # Sample data preview
        print("\n📋 Sample Data Preview:")
        spark.sql("SELECT * FROM kaustavpaul_demo.adventureworks.DimDate LIMIT 5").show()
        
    except Exception as e:
        print(f"⚠️  Sample queries failed: {str(e)}")
    
    print("\n✅ Complete setup finished!")
    return spark

if __name__ == "__main__":
    # Default: Just create tables (Step 1 complete)
    spark = main()
    
    # Keep session open for interactive use
    print(f"\n💡 Spark session is active. Use 'spark' object for queries.")
    print("📋 Available functions:")
    print("  • get_volume_files(spark) - List available CSV files")
    print("  • load_adventure_works_data(spark) - Load dimension/fact data")
    print("  • load_sales_data(spark) - Load sales fact tables")
    print("  • run_complete_setup() - Run everything including data loading")
    print("\nTo close: spark.stop()")
