# src/utils.py
import logging
import os
from datetime import datetime

def setup_logging(log_file='logs/data_pipeline.log', level=logging.INFO):
    """
    Sets up a logger that outputs to console and a file.
    """
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("Logging setup complete.")
    return logger

logger = setup_logging()

def get_output_base_path(project_root_dir="."):
    """
    Generates a unique, timestamped base path for outputs for the current run.
    E.g., telco_insights_project/output/YYYY-MM-DD_HH-MM-SS_run/
    """
    today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(project_root_dir, 'output', f'{today_str}_run')
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created output directory for this run: {output_dir}")
    return output_dir

def get_stage_output_path(base_output_path, stage_name):
    """
    Generates and ensures the existence of a stage-specific output directory.
    E.g., .../YYYY-MM-DD_HH-MM-SS_run/raw/
    """
    stage_dir = os.path.join(base_output_path, stage_name)
    os.makedirs(stage_dir, exist_ok=True)
    logger.info(f"Created output directory for stage '{stage_name}': {stage_dir}")
    return stage_dir

def save_dataframe_to_csv(df, path, file_name_prefix, stage_name):
    """
    Saves a DataFrame to a CSV file with a standardized naming convention.
    """
    if df is None:
        logger.warning(f"Attempted to save a None DataFrame for stage '{stage_name}'. Skipping.")
        return None

    file_date_str = datetime.now().strftime("%Y-%m-%d")
    full_file_name = f"{file_name_prefix}_{stage_name}_{file_date_str}.csv"
    full_path = os.path.join(path, full_file_name)

    try:
        df.to_csv(full_path, index=False)
        logger.info(f"Successfully saved {stage_name} data to: {full_path}")
        return full_path
    except Exception as e:
        logger.error(f"Error saving {stage_name} data to CSV at {full_path}: {e}", exc_info=True)
        return None