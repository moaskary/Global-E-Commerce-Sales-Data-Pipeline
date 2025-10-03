# Import PySpark types and functions
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round, sum as spark_sum, count, avg

# Define a constant for currency conversion for clarity and easy maintenance
GBP_TO_USD_RATE = 1.22 

def clean_data(df: DataFrame) -> DataFrame:
    """
    Performs data cleaning on the input DataFrame.
    - Drops rows with null 'Description'.
    - Filters out rows with zero or negative 'Quantity'.
    - Adds a 'TotalPrice' column.
    
    Args:
        df (DataFrame): The raw Spark DataFrame.
        
    Returns:
        DataFrame: The cleaned Spark DataFrame.
    """
    print("Cleaning data...")
    
    # Drop rows where 'Description' is null
    cleaned_df = df.na.drop(subset=["Description"])
    
    # Filter for rows where Quantity is greater than 0
    cleaned_df = cleaned_df.filter(col("Quantity") > 0)
    
    # Create the 'TotalPrice' column by multiplying 'Quantity' and 'UnitPrice'
    cleaned_df = cleaned_df.withColumn("TotalPrice", round(col("Quantity") * col("UnitPrice"), 2))
    
    print("Data cleaning complete.")
    return cleaned_df

def enrich_data(df: DataFrame) -> DataFrame:
    """
    Enriches the data by adding new columns.
    - Converts currency from GBP to USD.
    - Adds a 'Country' column (since all data is from UK). In a real-world scenario,
      this might involve a lookup or more complex logic.
      
    Args:
        df (DataFrame): The cleaned Spark DataFrame.
        
    Returns:
        DataFrame: The enriched Spark DataFrame.
    """
    print("Enriching data...")

    # Add a 'UnitPriceUSD' column based on the conversion rate
    enriched_df = df.withColumn("UnitPriceUSD", round(col("UnitPrice") * lit(GBP_TO_USD_RATE), 2))
    
    # Add a 'TotalPriceUSD' column
    enriched_df = enriched_df.withColumn("TotalPriceUSD", round(col("TotalPrice") * lit(GBP_TO_USD_RATE), 2))
    
    print("Data enrichment complete.")
    return enriched_df

def create_customer_aggregates(df: DataFrame) -> DataFrame:
    """
    Creates customer-level aggregates.
    
    Args:
        df (DataFrame): The enriched Spark DataFrame.
        
    Returns:
        DataFrame: A DataFrame with customer aggregates.
    """
    print("Creating customer aggregates...")
    
    customer_agg_df = df.groupBy("CustomerID", "Country").agg(
        spark_sum("TotalPriceUSD").alias("TotalSpentUSD"),
        count("InvoiceNo").alias("TotalTransactions"),
        avg("TotalPriceUSD").alias("AvgTransactionValueUSD")
    ).withColumn("AvgTransactionValueUSD", round(col("AvgTransactionValueUSD"), 2)) # Round the average for neatness
    
    print("Customer aggregates created.")
    return customer_agg_df

def create_product_aggregates(df: DataFrame) -> DataFrame:
    """
    Creates product-level aggregates.
    
    Args:
        df (DataFrame): The enriched Spark DataFrame.
        
    Returns:
        DataFrame: A DataFrame with product aggregates.
    """
    print("Creating product aggregates...")
    
    product_agg_df = df.groupBy("StockCode", "Description").agg(
        spark_sum("Quantity").alias("TotalUnitsSold"),
        spark_sum("TotalPriceUSD").alias("TotalRevenueUSD")
    )
    
    print("Product aggregates created.")
    return product_agg_df

def run_transformations(spark, input_path: str) -> dict[str, DataFrame]:
    """
    Main function to run all transformations.
    
    Args:
        spark (SparkSession): The active Spark session.
        input_path (str): The path to the input Parquet data.
        
    Returns:
        dict[str, DataFrame]: A dictionary of transformed DataFrames.
    """
    # Read the data ingested in the previous step
    print(f"Reading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    # Apply transformations sequentially
    cleaned_df = clean_data(df)
    enriched_df = enrich_data(cleaned_df)
    
    # Create aggregate tables
    customer_aggregates = create_customer_aggregates(enriched_df)
    product_aggregates = create_product_aggregates(enriched_df)
    
    # Return a dictionary of the final dataframes for the runner to save
    return {
        "fact_sales": enriched_df,
        "dim_customers": customer_aggregates,
        "dim_products": product_aggregates
    }