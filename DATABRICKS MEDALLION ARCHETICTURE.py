#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.oauth2.service_account import Credentials
from google.cloud import storage
import os

class GoogleCloudStorageClient:
    def __init__(self, key_file_path: str):
        self.credentials = self._authenticate_with_service_account(key_file_path)
        self.storage_client = self._initialize_storage_client()

    def _authenticate_with_service_account(self, key_file_path: str) -> Credentials:
        if not os.path.exists(key_file_path):
            raise FileNotFoundError(f"Service account key file not found at: {key_file_path}")
        
        return Credentials.from_service_account_file(key_file_path)

    def _initialize_storage_client(self) -> storage.Client:
        return storage.Client(credentials=self.credentials, project=self.credentials.project_id)

# Usage
key_file_path = "/Workspace/Users/emmanuelihetu750@gmail.com/icezy.json"
gcs_client = GoogleCloudStorageClient(key_file_path)


# In[ ]:


from google.cloud import storage
from typing import List

class GoogleCloudStorageClient:
    def __init__(self, key_file_path: str):
        self.credentials = self._authenticate_with_service_account(key_file_path)
        self.storage_client = self._initialize_storage_client()

    def _authenticate_with_service_account(self, key_file_path: str) -> storage.Client:
        """Authenticate with the service account key file."""
        if not os.path.exists(key_file_path):
            raise FileNotFoundError(f"Service account key file not found at: {key_file_path}")
        
        return storage.Client.from_service_account_json(key_file_path)

    def _initialize_storage_client(self) -> storage.Client:
        """Initialize the Google Cloud Storage client."""
        return storage.Client(credentials=self.credentials, project=self.credentials.project_id)

    def list_buckets(self) -> List[str]:
        """List all buckets in the GCS project."""
        return [bucket.name for bucket in self.storage_client.list_buckets()]

    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Retrieve a specific bucket by name."""
        bucket = self.storage_client.bucket(bucket_name)
        if not bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist.")
        return bucket

    def list_files_in_bucket(self, bucket_name: str) -> List[str]:
        """List all files in the specified bucket."""
        bucket = self.get_bucket(bucket_name)
        return [blob.name for blob in bucket.list_blobs()]

# Usage
key_file_path = "/Workspace/Users/emmanuelihetu750@gmail.com/icezy.json"
gcs_client = GoogleCloudStorageClient(key_file_path)

# List all buckets
bucket_names = gcs_client.list_buckets()
for name in bucket_names:
    print(f"Bucket: {name}")

# List all files in a specific bucket
bucket_name = 'keyrus_data'
file_names = gcs_client.list_files_in_bucket(bucket_name)
for file_name in file_names:
    print(f"File: {file_name}")


# In[ ]:


import os
from pathlib import Path

class DirectoryManager:
    def __init__(self, directory_path: str):
        self.directory_path = Path(directory_path)
        self.create_directory()

    def create_directory(self):
        """Create the directory if it does not exist."""
        try:
            self.directory_path.mkdir(parents=True, exist_ok=True)
            print(f"Directory created at: {self.directory_path}")
        except OSError as e:
            raise RuntimeError(f"Failed to create directory at {self.directory_path}: {e}")

class DatabricksFileSystem:
    @staticmethod
    def copy_to_dbfs(local_path: str, dbfs_path: str):
        """Copy a file from local filesystem to DBFS."""
        try:
            dbutils.fs.cp(f"file://{local_path}", dbfs_path)
            print(f"File copied to DBFS at: {dbfs_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to copy file to DBFS: {e}")

# Usage
directory_path = '/dbfs/tmp/'
local_file_path = '/local/path/to/keyrus_sales.csv'
dbfs_file_path = 'dbfs:/tmp/keyrus_sales.csv'

# Create the directory if it doesn't exist
DirectoryManager(directory_path)

# Copy the file to DBFS
DatabricksFileSystem.copy_to_dbfs(local_file_path, dbfs_file_path)


# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class SparkCSVLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv_to_df(self, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """Load a CSV file from DBFS into a Spark DataFrame."""
        try:
            df = self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)
            print(f"CSV file loaded into DataFrame from: {file_path}")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV into DataFrame: {e}")

    def display_df(self, df: DataFrame, num_rows: int = 20):
        """Display the content of the DataFrame."""
        try:
            df.show(num_rows)
        except Exception as e:
            raise RuntimeError(f"Failed to display DataFrame content: {e}")

# Usage
spark = SparkSession.builder.appName("CSVLoaderApp").getOrCreate()
csv_loader = SparkCSVLoader(spark)

# Load the CSV file into a DataFrame
file_path = "dbfs:/tmp/keyrus_sales.csv"
data_df = csv_loader.load_csv_to_df(file_path)

# Show the content of the DataFrame
csv_loader.display_df(data_df)


# In[ ]:


import re
from pyspark.sql import DataFrame

class DataFrameCleaner:
    @staticmethod
    def clean_column_name(col_name: str) -> str:
        """Clean a column name by replacing invalid characters with underscores."""
        return re.sub(r'[ ,;{}()\n\t=]', '_', col_name)

    def clean_column_names(self, df: DataFrame) -> DataFrame:
        """Apply the cleaning function to all column names in the DataFrame."""
        cleaned_columns = [df[col].alias(self.clean_column_name(col)) for col in df.columns]
        cleaned_df = df.select(cleaned_columns)
        print("Column names cleaned.")
        return cleaned_df

# Usage
df_cleaner = DataFrameCleaner()

# Clean the column names in the DataFrame
cleaned_data_df = df_cleaner.clean_column_names(data_df)

# Verify the cleaned column names
print("Cleaned Columns:", cleaned_data_df.columns)


# In[ ]:


from pyspark.sql import DataFrame, SparkSession

class DeltaTableManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def save_as_delta(self, df: DataFrame, save_path: str, mode: str = "overwrite"):
        """Save the DataFrame as a Delta table at the specified path."""
        try:
            df.write.format("delta").mode(mode).save(save_path)
            print(f"Data successfully saved in Delta format at: {save_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to save DataFrame as Delta: {e}")

    def load_delta_table(self, load_path: str) -> DataFrame:
        """Load a Delta table from the specified path into a DataFrame."""
        try:
            df = self.spark.read.format("delta").load(load_path)
            print(f"Delta table loaded from: {load_path}")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load Delta table: {e}")

    def create_temp_view(self, df: DataFrame, view_name: str):
        """Create a temporary view for the DataFrame."""
        try:
            df.createOrReplaceTempView(view_name)
            print(f"Temporary view '{view_name}' created.")
        except Exception as e:
            raise RuntimeError(f"Failed to create temporary view: {e}")

    def query_view(self, query: str):
        """Run a SQL query on a temporary view and display the results."""
        try:
            result_df = self.spark.sql(query)
            result_df.show()
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")

# Usage
spark = SparkSession.builder.appName("DeltaTableApp").getOrCreate()
delta_manager = DeltaTableManager(spark)

# Path to save the Bronze layer data
bronze_path = "dbfs:/mnt/bronze/keyrus_sales"

# Save the cleaned data to the Bronze table in Delta format
delta_manager.save_as_delta(cleaned_data_df, bronze_path)

# Load the Delta table
bronze_df = delta_manager.load_delta_table(bronze_path)

# Create a temporary view for the Bronze layer
delta_manager.create_temp_view(bronze_df, "bronze_sales")

# Query the temporary view
delta_manager.query_view("SELECT * FROM bronze_sales")


# In[ ]:


from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class SilverLayerProcessor:
    def __init__(self, delta_manager: DeltaTableManager):
        self.delta_manager = delta_manager

    def process_silver_layer(self, bronze_df: DataFrame, silver_path: str) -> DataFrame:
        """Process the Silver layer by filtering out cancelled orders and calculating the total amount."""
        try:
            silver_df = bronze_df.filter(col("Order_Status") != "Cancelled") \
                                 .withColumn("TotalAmount", col("Quantity") * col("Unit_Price"))
            print("Silver layer processed.")
            
            # Save the processed data to the Silver Delta table
            self.delta_manager.save_as_delta(silver_df, silver_path)
            return silver_df
        except Exception as e:
            raise RuntimeError(f"Failed to process Silver layer: {e}")

# Usage
silver_path = "dbfs:/mnt/silver/keyrus_sales"
silver_processor = SilverLayerProcessor(delta_manager)

# Process the Silver layer
silver_data_df = silver_processor.process_silver_layer(bronze_df, silver_path)

# Create a temporary view for the Silver layer
delta_manager.create_temp_view(silver_data_df, "silver_sales")

# Query the Silver layer
delta_manager.query_view("SELECT * FROM silver_sales")


# In[ ]:


from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum

class GoldLayerProcessor:
    def __init__(self, delta_manager: DeltaTableManager):
        self.delta_manager = delta_manager

    def process_gold_layer(self, silver_df: DataFrame, gold_path: str) -> DataFrame:
        """Process the Gold layer by aggregating total sales per product."""
        try:
            # Aggregate the total sales and quantity per product
            gold_df = silver_df.groupBy("Product_ID") \
                               .agg(
                                   _sum("TotalAmount").alias("Total_Sales"),
                                   _sum("Quantity").alias("Total_Quantity_Sold")
                               )
            print("Gold layer processed.")
            
            # Save the aggregated data to the Gold Delta table
            self.delta_manager.save_as_delta(gold_df, gold_path)
            return gold_df
        except Exception as e:
            raise RuntimeError(f"Failed to process Gold layer: {e}")

# Usage
gold_path = "dbfs:/mnt/gold/keyrus_sales"
gold_processor = GoldLayerProcessor(delta_manager)

# Process the Gold layer
gold_data_df = gold_processor.process_gold_layer(silver_data_df, gold_path)

# Create a temporary view for the Gold layer
delta_manager.create_temp_view(gold_data_df, "gold_sales")

# Query the Gold layer
delta_manager.query_view("SELECT * FROM gold_sales")


# In[ ]:


-- SQL Query to retrieve the top 10 products by total sales from the Gold layer
-- This query orders the products by their total sales in descending order
-- and limits the result to the top 10 entries.

SELECT Product_ID, Total_Sales, Total_Quantity_Sold 
FROM gold_sales 
ORDER BY Total_Sales DESC 
LIMIT 10;

