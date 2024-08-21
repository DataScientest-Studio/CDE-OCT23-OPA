import warnings
warnings.filterwarnings("ignore")
from mlflow import MlflowClient, set_tracking_uri
import mlflow
from typing import Tuple
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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


def Daily_model(exchange):
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
    
    # Calculate the cutoff_date as the first day of 6 months ago
    cutoff_date = (datetime.now() - relativedelta(months=12)).replace(day=1)
    
    print(cutoff_date)

    # Use the provided train_test_split function
    X_train, y_train, X_test, y_test = train_test_split(
        df,
        cutoff_date=cutoff_date,
        target_column_name='Close'
    )

    # Use the provided train_test_split function
   # X_train, y_train, X_test, y_test = train_test_split(
   #     df,
   #     cutoff_date=datetime(2024, 1, 1, 0, 0, 0),
   #     target_column_name='Close'
   # )

    X_train = X_train.apply(lambda col: col.astype(int) if col.name != 'id_date' else col)
    X_test = X_test.apply(lambda col: col.astype(int) if col.name != 'id_date' else col)
 
    X_train_only_numeric = X_train.drop(columns = 'id_date')
    X_test_only_numeric = X_test.drop(columns = 'id_date')
    
    y_train = y_train.astype(int)
    y_test = y_test.astype(int)

    
    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')
    
    # Define tracking_uri to point to the MLflow server in Docker
    #client = MlflowClient(tracking_uri="http://localhost:5000")
    set_tracking_uri("http://localhost:5000") 
    
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment(f"Daily_Model_Original_LR_{exchange}")
    
    #run_name = "second_run"
    
    artifact_path_LR = f"daily_model_original_LR_{exchange}"

    # Linear Regression
    model = 'daily_model_original_LR'
    LR = LinearRegression()
    LR.fit(X_train_only_numeric, y_train)
    regressor_pred_test = LR.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, regressor_pred_test)
    mse = mean_squared_error(y_test, regressor_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, regressor_pred_test)
    metrics_LR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    # Store information in tracking server
    with mlflow.start_run(run_name = f"Daily_Model_Original_LR_{exchange}_{execution_date}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics_LR)
        mlflow.sklearn.log_model(
            sk_model=LR, input_example=X_test_only_numeric, artifact_path=artifact_path_LR
        )
        
    print(f"Run: {model} - {exchange}")
        
    
        
    # # XGBoost
    
    # # Define experiment name, run name and artifact_path name
    # apple_experiment = mlflow.set_experiment(f"Daily_Model_Original_XGB_{exchange}")
    
    # #run_name = "second_run"
    
    # artifact_path_XGB = f"daily_model_original_XGB_{exchange}"
    
    # model = 'daily_model_original_XGB'
    # XGB = xgb.XGBRegressor()
    # XGB.fit(X_train_only_numeric, y_train)
    # XGB_pred_test = XGB.predict(X_test_only_numeric)
    
    # mae = mean_absolute_error(y_test, XGB_pred_test)
    # mse = mean_squared_error(y_test, XGB_pred_test)
    # rmse = np.sqrt(mse)
    # r2 = r2_score(y_test, XGB_pred_test)
    # metrics_XGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    # # Store information in tracking server
    # with mlflow.start_run(run_name = f"Daily_Model_Original_XGB_{exchange}_{execution_date}") as run:
    #     #mlflow.log_params(params)
    #     mlflow.log_metrics(metrics_XGB)
    #     mlflow.sklearn.log_model(
    #         sk_model=XGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
    #     )
    # print(f"Run: {model} - {exchange}")
        
    # # LGB
    
    # # Define experiment name, run name and artifact_path name
    # apple_experiment = mlflow.set_experiment(f"Daily_Model_Original_LGB_{exchange}")
    
    # #run_name = "second_run"
    
    # artifact_path_XGB = f"daily_model_original_LGB_{exchange}"
    
    # model = 'daily_model_original_LGB'
    # LGB = lgb.LGBMRegressor()
    # LGB.fit(X_train_only_numeric, y_train)
    # LGB_pred_test = LGB.predict(X_test_only_numeric)
    
    # mae = mean_absolute_error(y_test, LGB_pred_test)
    # mse = mean_squared_error(y_test, LGB_pred_test)
    # rmse = np.sqrt(mse)
    # r2 = r2_score(y_test, LGB_pred_test)
    # metrics_LGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    # # Store information in tracking server
    # with mlflow.start_run(run_name = f"Daily_Model_Original_LGB_{exchange}_{execution_date}") as run:
    #     #mlflow.log_params(params)
    #     mlflow.log_metrics(metrics_LGB)
    #     mlflow.sklearn.log_model(
    #         sk_model=LGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
    #     )
        
    # print(f"Run: {model} - {exchange}")
        

    
    


def daily_model(exchanges):
    
    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        Daily_model(exchange) 

def main():
    
    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    daily_model(exchanges)
    
if __name__ == "__main__":
    main()
