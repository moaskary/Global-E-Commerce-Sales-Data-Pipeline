import argparse
from pyspark.sql import SparkSession
from pathlib import Path
from transformations import run_transformations
import os
import socket

def get_local_ip():
    """
    A robust method to get the local IP address of the machine.
    It works by creating a temporary socket and connecting to a public DNS server.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't have to be reachable; it's just to find the outbound IP
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        # Fallback if no network connection is available
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def main():
    """
    The main entry point for the Spark job.
    """
    # Set the correct JAVA_HOME
    java_home_path = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
    os.environ['JAVA_HOME'] = java_home_path

    # Use the robust function to get the host IP
    host_ip = get_local_ip()
    print(f"--- Using host IP for Spark Driver: {host_ip} ---")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="PySpark Transformation Job")
    parser.add_argument("--input", required=True, help="Input directory of Parquet files")
    parser.add_argument("--output", required=True, help="Output directory for transformed data")
    args = parser.parse_args()

    input_path = args.input
    output_base_path = args.output
    
    # --- Spark Session Initialization ---
    spark = SparkSession.builder \
        .master("spark://localhost:7077") \
        .appName("ECommerceTransformation") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.host", host_ip) \
        .getOrCreate()
    
    print("Spark session created successfully.")

    try:
        transformed_dfs = run_transformations(spark, input_path)
        
        for name, df in transformed_dfs.items():
            output_path = f"{output_base_path}/{name}"
            print(f"Saving '{name}' DataFrame to {output_path}...")
            
            Path(output_path).mkdir(parents=True, exist_ok=True)
            
            df.write.mode("overwrite").parquet(output_path)
            print(f"Successfully saved '{name}'.")
            
    except Exception as e:
        print(f"An error occurred during the Spark job: {e}")
    finally:
        print("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()


# import argparse
# from pyspark.sql import SparkSession
# from pathlib import Path
# from transformations import run_transformations # Import our logic

# def main():
#     """
#     The main entry point for the Spark job.
#     """
#     # --- Argument Parsing ---
#     # This allows us to pass input and output paths from the command line
#     parser = argparse.ArgumentParser(description="PySpark Transformation Job")
#     parser.add_argument("--input", required=True, help="Input directory of Parquet files")
#     parser.add_argument("--output", required=True, help="Output directory for transformed data")
#     args = parser.parse_args()

#     input_path = args.input
#     output_base_path = args.output
    
#     # --- Spark Session Initialization ---
#     # This is how we connect to our Spark cluster.
#     # .master("spark://spark-master:7077") tells Spark where the master node is.
#     # 'spark-master' is the hostname we defined in docker-compose.yml.
#     # .appName sets a name for our job, which will appear in the Spark UI.
#     spark = SparkSession.builder \
#         .master("spark://localhost:7077") \
#         .appName("ECommerceTransformation") \
#         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
#         .getOrCreate()

#     try:
#         # Run the core transformation logic imported from the other file
#         transformed_dfs = run_transformations(spark, input_path)
        
#         # --- Save the results ---
#         # Loop through the dictionary of DataFrames and save each one.
#         # We are saving in Parquet format again, which is standard for Spark.
#         # 'overwrite' mode means if the directory already exists, it will be replaced.
#         for name, df in transformed_dfs.items():
#             output_path = f"{output_base_path}/{name}"
#             print(f"Saving '{name}' DataFrame to {output_path}...")
            
#             # Ensure the output directory exists
#             Path(output_path).mkdir(parents=True, exist_ok=True)
            
#             df.write.mode("overwrite").parquet(output_path)
#             print(f"Successfully saved '{name}'.")
            
#     except Exception as e:
#         print(f"An error occurred during the Spark job: {e}")
#     finally:
#         # Always stop the Spark session to release resources
#         print("Stopping Spark session.")
#         spark.stop()

# if __name__ == "__main__":
#     main()