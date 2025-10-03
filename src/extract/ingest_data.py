import pandas as pd
import argparse # For parsing command-line arguments
from pathlib import Path # For handling file paths in a system-agnostic way

# --- A Note on Best Practices ---
# It's good practice to make scripts runnable from the command line.
# The `argparse` library helps us define arguments that the script expects.
# Here, we expect one argument: the path to the input data file.

# src/extract/ingest_data.py

def ingest_data(input_path: str, output_path: str):
    """
    Reads raw e-commerce data, performs initial cleaning, 
    and saves it to a Parquet file.
    
    Args:
        input_path (str): The file path for the raw data (.xlsx).
        output_path (str): The destination path for the processed Parquet file.
    """
    print(f"Reading data from {input_path}...")
    
    df = pd.read_excel(input_path, engine='openpyxl')
    print("Data read successfully. DataFrame shape:", df.shape)

    # --- Initial Data Cleaning & Quality Checks ---

    # 1. Drop rows with missing CustomerID.
    df.dropna(subset=['CustomerID'], inplace=True)
    print(f"Dropped rows with missing CustomerID. New shape: {df.shape}")

    # 2. Filter out cancelled orders.
    df = df[~df['InvoiceNo'].astype(str).str.startswith('C')]
    print(f"Removed cancelled orders. New shape: {df.shape}")
    
    # 3. Ensure key columns have a consistent string type to prevent save errors.
    # This fixes the ArrowTypeError by making the column uniform.
    df['StockCode'] = df['StockCode'].astype(str) # NEW: Add this line
    df['CustomerID'] = df['CustomerID'].astype(str) # Also a good idea for CustomerID

    # 4. Ensure the output directory exists.
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # 5. Save the cleaned data to a Parquet file.
    print(f"Saving cleaned data to {output_path}...")
    df.to_parquet(output_path, index=False)
    
    print("Ingestion complete. Cleaned data saved.")


# This block allows the script to be run from the command line.
if __name__ == "__main__":
    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Data Ingestion Script for E-commerce Data")
    
    # Define the command-line arguments the script will accept
    parser.add_argument("--input", type=str, required=True, help="Path to the input data file (.xlsx)")
    parser.add_argument("--output", type=str, required=True, help="Path to save the output Parquet file")
    
    # Parse the arguments provided by the user
    args = parser.parse_args()
    
    # Call the main function with the provided arguments
    ingest_data(input_path=args.input, output_path=args.output)