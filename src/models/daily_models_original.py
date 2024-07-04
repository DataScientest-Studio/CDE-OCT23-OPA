import warnings
warnings.filterwarnings("ignore")
from mlflow import MlflowClient
import mlflow
from typing import Tuple
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
import pyarrow
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import argparse
import os


def train_test_split(
    df: pd.DataFrame,
    cutoff_date: datetime,
    target_column_name: str,
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    train_data = df[df.id_date < cutoff_date].reset_index(drop=True)
    test_data = df[df.id_date >= cutoff_date].reset_index(drop=True)

    X_train = train_data.drop(columns=[target_column_name])
    y_train = train_data[target_column_name]
    X_test = test_data.drop(columns=[target_column_name])
    y_test = test_data[target_column_name]

    return X_train, y_train, X_test, y_test


def Daily_model_LR(exchange):
    temporality = 'daily'
    
    connection = mysql.connector.connect(
        user='root',
        password='root',
        host='localhost',
        port=3306,
        database='Historical_Data'
    )
    print("MySQL DB Connected")
    
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_DAILY_DATA WHERE Exchange = '{exchange}'")

    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    df_original = pd.DataFrame(results, columns=columns)
    df = df_original
    df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'id_date']]

    # Use the provided train_test_split function
    X_train, y_train, X_test, y_test = train_test_split(
        df,
        cutoff_date=datetime(2023, 6, 1, 0, 0, 0),
        target_column_name='Close'
    )

    X_train = X_train.apply(lambda col: col.astype(int) if col.name != 'id_date' else col)
    X_test = X_test.apply(lambda col: col.astype(int) if col.name != 'id_date' else col)
 
    X_train_only_numeric = X_train.drop(columns = 'id_date')
    X_test_only_numeric = X_test.drop(columns = 'id_date')
    
    y_train = y_train.astype(int)
    y_test = y_test.astype(int)

    
    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')
    
    # Define tracking_uri
    client = MlflowClient(tracking_uri="http://127.0.0.1:8080")
    
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment("Daily_Model_Original_LR")
    #run_name = "second_run"
    artifact_path = "daily_model_original_LR"

    # Linear Regression
    LR = LinearRegression()
    LR.fit(X_train_only_numeric, y_train)
    regressor_pred_test = LR.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, regressor_pred_test)
    mse = mean_squared_error(y_test, regressor_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, regressor_pred_test)
    metrics = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    

    # Store information in tracking server
    with mlflow.start_run(run_name = f"Daily_Model_Original_LR_{exchange}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(
            sk_model=LR, input_example=X_test_only_numeric, artifact_path=artifact_path
        )
    


def main(exchanges):
    
    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        metrics_df = Daily_model_LR(exchange) 
        print(f"Run: Daily model Original - LR - {exchange}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    main(exchanges)
