-- sql/initial_churn_data.sql
SELECT
    customerID,
    gender,
    "SeniorCitizen", -- Use double quotes if column names are mixed-case or contain special characters
    Partner,
    Dependents,
    tenure,
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    Contract,
    "PaperlessBilling",
    "PaymentMethod",
    "MonthlyCharges",
    "TotalCharges",
    Churn
FROM
    customer_churn
WHERE
    "tenure" >= 0; -- Changed from > 0 to >= 0 to include new customers (tenure 0) in the raw data