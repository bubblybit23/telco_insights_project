# src/data_loader.py
import pandas as pd
import psycopg2
from psycopg2 import Error
from src.utils import logger, get_stage_output_path, save_dataframe_to_csv
import os
from dotenv import load_dotenv
import subprocess


load_dotenv()

def download_kaggle_dataset(dataset_name, destination_path):
    """
    Downloads a dataset from Kaggle using the Kaggle API CLI.
    The dataset will be unzipped automatically.
    Assumes Kaggle API credentials (kaggle.json) are set up correctly.
    """
    logger.info(f"Attempting to download Kaggle dataset '{dataset_name}' to '{destination_path}'...")
    os.makedirs(destination_path, exist_ok=True) # Ensure destination directory exists

    try:
        # Construct the Kaggle CLI command
        # -d: dataset identifier
        # -p: path to download to
        # --unzip: unzip the downloaded archive
        # Define the absolute path to kaggle.exe within your virtual environment
        # Make sure this path is accurate for your setup!
        # Use forward slashes (/) or double backslashes (\\) for paths in Python strings
        kaggle_executable_path = r"C:\Users\nikki\Documents\telco_insights_project\.venv\Scripts\kaggle.exe" # Use 'r' for raw string or replace \ with /

        command = [
            kaggle_executable_path, # <--- MODIFICATION HERE: Use the absolute path
            'datasets', 'download',
            '-d', dataset_name,
            '-p', destination_path,
            '--unzip'
        ]

        # Run the command and capture output
        # text=True captures output as string, check=True raises CalledProcessError for non-zero exit codes
        result = subprocess.run(command, capture_output=True, text=True, check=True)

        logger.info(f"Kaggle download successful for {dataset_name}.")
        logger.debug(f"Kaggle CLI Output: {result.stdout}")

        # Kaggle sometimes downloads multiple files or names
        # For 'telco-customer-churn', it usually downloads 'WA_Fn-UseC_-Telco-Customer-Churn.csv'
        downloaded_file_name = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
        full_downloaded_path = os.path.join(destination_path, downloaded_file_name)

        if os.path.exists(full_downloaded_path):
            logger.info(f"Expected file '{downloaded_file_name}' found at '{full_downloaded_path}'.")
            return full_downloaded_path
        else:
            logger.error(f"Expected file '{downloaded_file_name}' not found after download and unzip.")
            # Log what was actually downloaded
            downloaded_files = os.listdir(destination_path)
            logger.debug(f"Files in destination '{destination_path}': {downloaded_files}")
            return None

    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading Kaggle dataset (CLI error): {e}", exc_info=True)
        logger.error(f"Kaggle CLI Error Output (stdout): {e.stdout}")
        logger.error(f"Kaggle CLI Error Output (stderr): {e.stderr}")
        return None
    except FileNotFoundError:
        logger.error("Kaggle CLI command not found. Please ensure 'kaggle' is installed and in your PATH.", exc_info=True)
        return None
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Kaggle download: {e}", exc_info=True)
        return None

def load_data_from_csv(file_path, output_base_path=None):
    """
    Loads data from a CSV file into a Pandas DataFrame.
    Includes robust error handling, logging, and saves raw data.
    """
    logger.info(f"Attempting to load data from CSV: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded data from {file_path}. Shape: {df.shape}")
        logger.debug(f"CSV Header: {df.columns.tolist()}")
        logger.debug(f"First 5 rows:\n{df.head().to_string()}")

        if output_base_path:
            raw_output_path = get_stage_output_path(output_base_path, 'raw')
            save_dataframe_to_csv(df, raw_output_path, 'telco_customer_churn', 'raw')

        return df
    except FileNotFoundError:
        logger.error(f"Error: The file '{file_path}' was not found. Please check the path.", exc_info=True)
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Error: CSV file '{file_path}' is empty.", exc_info=True)
        return None
    except pd.errors.ParserError:
        logger.error(f"Error: Could not parse CSV file '{file_path}'. Check file format.", exc_info=True)
        return None
    except Exception as e:
        logger.critical(f"An unexpected error occurred while loading the CSV: {e}", exc_info=True)
        return None

def load_data_from_postgres(query, output_base_path=None):
    """
    Loads data from PostgreSQL into a Pandas DataFrame using credentials from .env.
    Includes robust error handling, logging, and saves raw data.
    """
    db_name = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    if not all([db_name, user, password, host, port]):
        logger.error("Database credentials not found in environment variables. Please check your .env file.")
        return None

    logger.info(f"Attempting to connect to PostgreSQL database: {db_name}@{host}:{port}")
    conn = None
    df = None
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logger.info("Successfully connected to PostgreSQL database.")
        logger.info(f"Executing SQL query: {query[:100]}...")
        df = pd.read_sql_query(query, conn)
        logger.info(f"Successfully loaded data from PostgreSQL. Shape: {df.shape}")
        logger.debug(f"PostgreSQL Data types:\n{df.info(verbose=True, buf=pd.io.common.StringIO()).getvalue()}")

        if output_base_path:
            raw_output_path = get_stage_output_path(output_base_path, 'raw')
            save_dataframe_to_csv(df, raw_output_path, 'telco_customer_churn', 'raw')

    except Error as e:
        logger.error(f"Error connecting to or querying PostgreSQL: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during PostgreSQL data loading: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")
    return df