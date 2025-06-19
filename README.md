# Telco Customer Churn Prediction - Data Engineering Pipeline

## Project Overview

This project aims to build a robust data engineering pipeline to process telecommunications customer data, ultimately leading to the development of a machine learning model capable of predicting customer churn. Predicting churn allows companies to proactively identify at-risk customers and implement retention strategies, thereby improving customer lifetime value and business profitability.

## Project Goal

The primary goal is to predict customer churn (binary classification problem: Churn/No Churn) for a telecom company based on various customer attributes and service usage patterns.

## Current Project Status: Phase 1 - Completed!

As of **[Current Date, e.g., June 19, 2025]**, Phase 1 of this project, focusing on **Data Ingestion and Cleaning**, has been successfully completed.

### Phase 1 Achievements:

* **Project Setup:** Initial project structure, virtual environment, and logging system established.
* **Data Ingestion:** Successfully downloaded raw customer churn data from Kaggle (`blastchar/telco-customer-churn`).
* **Data Cleaning & Transformation:** Performed essential data cleaning steps, including:
    * Converting `TotalCharges` to numeric type and handling missing values.
    * Encoding the `Churn` target variable ('Yes'/'No' to 1/0).
    * Dropping irrelevant `customerID` column.
* **Data Layering:** Data has been saved to different pipeline stages (`raw`, `staging`, `final` - CSV formats) for auditability and resilience.
* **Processed Data Storage:** Cleaned data has been saved in an optimized Parquet format (`telco_customer_churn_cleaned.parquet`) for efficient downstream processing.

## Next Steps (Future Phases)

The following phases are planned for future development:

* **Phase 2: Exploratory Data Analysis (EDA) & Feature Engineering:**
    * Deep dive into cleaned data characteristics.
    * Identify relationships and patterns.
    * Create new, impactful features for model training.
* **Phase 3: Model Development & Training:**
    * Select appropriate machine learning algorithms for churn prediction.
    * Train models on the prepared dataset.
    * Hyperparameter tuning.
* **Phase 4: Model Evaluation & Selection:**
    * Rigorously evaluate model performance using relevant metrics (e.g., Precision, Recall, F1-score, ROC-AUC).
    * Compare models and select the best one based on business objectives.
* **(Optional) Phase 5: Model Deployment & Monitoring:**
    * Deploy the best model to make real-time predictions.
    * Set up monitoring dashboards to track model performance in production.

## Project Setup and How to Run

1.  **Clone the Repository:**
    ```bash
    git clone [Your GitHub Repo URL]
    cd telco_insights_project
    ```
2.  **Create and Activate Virtual Environment:**
    ```bash
    python -m venv .venv
    # On Windows:
    .\.venv\Scripts\activate
    # On macOS/Linux:
    source ./.venv/bin/activate
    ```
3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    # If you don't have requirements.txt yet, you might need:
    # pip install pandas jupyter matplotlib seaborn scikit-learn kaggle pyarrow psycopg2-binary
    # (or whatever specific packages you've installed)
    ```
4.  **Configure Kaggle API (for data download):**
    * Go to Kaggle.com -> Account -> Create New API Token.
    * Place `kaggle.json` in `C:\Users\YourUsername\.kaggle\` (Windows) or `~/.kaggle/` (macOS/Linux).
5.  **Run the Jupyter Notebook:**
    ```bash
    jupyter notebook
    ```
    Open `01_data_ingestion_and_cleaning.ipynb` and run all cells sequentially.

## Project Structure

```
telco_insights_project/
├── .venv/                   # Python Virtual Environment
├── data/                    # Raw data downloaded from Kaggle (external data)
├── output/                  # All processed data and pipeline outputs by run_timestamp
│   └── 2025-XX-XX_HH-MM-SS_run/ # Timestamped output for each run
│       ├── raw/             # Copy of raw data
│       ├── staging/         # Data after initial cleaning
│       ├── final/           # Fully cleaned data (CSV)
│       └── processed_data/  # Fully cleaned data (Parquet)
├── src/                     # Source code for pipeline functions
│   ├── init.py
│   ├── data_cleaner.py      # Contains data cleaning logic
│   ├── data_loader.py       # Handles data ingestion (Kaggle, CSV, DB)
│   └── utils.py             # Utility functions (logging, paths)
├── sql/                     # SQL files (e.g., for PostgreSQL ingestion example)
├── 01_data_ingestion_and_cleaning.ipynb # Main Jupyter Notebook for Phase 1
├── requirements.txt         # Project dependencies
└── README.md                # Project README file
```

## Acknowledgments

* **Dataset:** Telco Customer Churn dataset available on Kaggle by `blastchar`.

---