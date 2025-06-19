# src/data_cleaner.py
import pandas as pd
from src.utils import logger, get_stage_output_path, save_dataframe_to_csv
import io

def clean_telco_data(df, output_base_path=None):
    """
    Performs specific cleaning and transformation steps for the Telco Churn dataset.
    Includes robust error handling, logging, and saves staging/final data.
    """
    if df is None:
        logger.warning("Input DataFrame is None. Cannot perform cleaning.")
        return None

    cleaned_df = df.copy()
    logger.info("Starting data cleaning and transformation process.")

    logger.info("Processing 'TotalCharges' column...")
    try:
        original_dtype = cleaned_df['TotalCharges'].dtype
        cleaned_df['TotalCharges'] = pd.to_numeric(cleaned_df['TotalCharges'], errors='coerce')
        logger.info(f"Converted 'TotalCharges' from {original_dtype} to numeric. "
                    f"Found {cleaned_df['TotalCharges'].isnull().sum()} NaN values (including coerced).")

        initial_nan_count = cleaned_df['TotalCharges'].isnull().sum()
        if initial_nan_count > 0:
            cleaned_df['TotalCharges'].fillna(0, inplace=True)
            logger.info(f"Filled {initial_nan_count} NaN values in 'TotalCharges' with 0.")
        logger.debug(f"Descriptive stats for 'TotalCharges' after cleaning:\n{cleaned_df['TotalCharges'].describe().to_string()}")
    except KeyError:
        logger.error("'TotalCharges' column not found in DataFrame.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error processing 'TotalCharges': {e}", exc_info=True)
        return None

    logger.info("Encoding 'Churn' column (Yes=1, No=0)...")
    try:
        unique_churn_before = cleaned_df['Churn'].unique()
        cleaned_df['Churn'] = cleaned_df['Churn'].replace({'Yes': 1, 'No': 0})
        unique_churn_after = cleaned_df['Churn'].unique()
        logger.info(f"Original 'Churn' values: {unique_churn_before}. Encoded 'Churn' values: {unique_churn_after}.")

        if not cleaned_df['Churn'].isin([0, 1]).all():
            unexpected_values = cleaned_df['Churn'][~cleaned_df['Churn'].isin([0, 1])].unique()
            logger.warning(f"'Churn' column contains unexpected values after encoding: {unexpected_values}.")
        else:
            logger.info("'Churn' column successfully encoded to 0/1.")
    except KeyError:
        logger.error("'Churn' column not found in DataFrame.", exc_info=True)
    except Exception as e:
        logger.error(f"Error encoding 'Churn' column: {e}", exc_info=True)

    logger.info("Checking for 'customerID' column to drop...")
    if 'customerID' in cleaned_df.columns:
        cleaned_df = cleaned_df.drop('customerID', axis=1)
        logger.info("Dropped 'customerID' column.")
    else:
        logger.warning("'customerID' column not found, skipped dropping.")

    logger.info("\n--- DataFrame State After Cleaning ---")
    logger.info(f"Shape after cleaning: {cleaned_df.shape}")
    # logger.info(f"Data types after cleaning:\n{cleaned_df.info(verbose=True, buf=io.StringIO()).getvalue()}")
    logger.info(f"Missing values per column after cleaning:\n{cleaned_df.isnull().sum().to_string()}")
    logger.debug(f"First 5 rows after cleaning:\n{cleaned_df.head().to_string()}")

    if output_base_path:
        staging_output_path = get_stage_output_path(output_base_path, 'staging')
        save_dataframe_to_csv(cleaned_df, staging_output_path, 'telco_customer_churn', 'staging')

        final_output_path = get_stage_output_path(output_base_path, 'final')
        save_dataframe_to_csv(cleaned_df, final_output_path, 'telco_customer_churn', 'final')

    logger.info("Data cleaning and transformation process completed.")
    return cleaned_df