import warnings
warnings.filterwarnings("ignore")
from mlflow import MlflowClient, set_tracking_uri
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
from dateutil.relativedelta import relativedelta

from functions_daily import get_cutoff_indices, transform_ts_data_into_features_and_target, train_test_split, ts_into_features_Daily


def mlflow_daily(exchange):
    
    X_test_only_numeric, X_train_only_numeric, y_test, y_train = ts_into_features_Daily(exchange)
    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')
    
    
    # Define tracking_uri to point to the MLflow server in Docker
    #client = MlflowClient(tracking_uri="http://localhost:5000")
    set_tracking_uri("http://localhost:5000") 
    
    
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment(f"ts_into_features_Daily_LR_{exchange}")
    #run_name = "second_run"
    artifact_path_LR = f"ts_into_features_Daily_LR_{exchange}"
    
    
    # Linear Regression
    model = 'ts_into_features_Daily_LR'
    LR = LinearRegression()
    LR.fit(X_train_only_numeric, y_train)
    regressor_pred_test = LR.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, regressor_pred_test)
    mse = mean_squared_error(y_test, regressor_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, regressor_pred_test)
    metrics_LR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    
    # Store information in tracking server
    with mlflow.start_run(run_name = f"ts_into_features_Daily_LR_{exchange}_{execution_date}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics_LR)
        mlflow.sklearn.log_model(
            sk_model=LR, input_example=X_test_only_numeric, artifact_path=artifact_path_LR
        )
        
    print(f"Run: {model} - {exchange}")
        
    # #XGBOOST
        
    # # Define experiment name, run name and artifact_path name
    # apple_experiment = mlflow.set_experiment(f"ts_into_features_Daily_XGB_{exchange}")
    # #run_name = "second_run"
    # artifact_path_XGB = f"ts_into_features_Daily_XGB_{exchange}"
    
    
    # # Linear Regression
    # model = 'ts_into_features_Daily_XGB'
    # XGB = xgb.XGBRegressor()
    # XGB.fit(X_train_only_numeric, y_train)
    # XGB_pred_test = XGB.predict(X_test_only_numeric)
    
    # mae = mean_absolute_error(y_test, XGB_pred_test)
    # mse = mean_squared_error(y_test, XGB_pred_test)
    # rmse = np.sqrt(mse)
    # r2 = r2_score(y_test, XGB_pred_test)
    # metrics_XGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    
    # # Store information in tracking server
    # with mlflow.start_run(run_name = f"ts_into_features_Daily_XGB_{exchange}_{execution_date}") as run:
    #     #mlflow.log_params(params)
    #     mlflow.log_metrics(metrics_XGB)
    #     mlflow.sklearn.log_model(
    #         sk_model=XGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
    #     )
        
    # print(f"Run: {model} - {exchange}")
        
    # #LGB
        
    # # Define experiment name, run name and artifact_path name
    # apple_experiment = mlflow.set_experiment(f"ts_into_features_Daily_LGB_{exchange}")
    # #run_name = "second_run"
    # artifact_path_XGB = f"ts_into_features_Daily_LGB_{exchange}"
    
    
    # # Linear Regression
    # model = 'ts_into_features_Daily_LGB'
    # LGB = lgb.LGBMRegressor()
    # LGB.fit(X_train_only_numeric, y_train)
    # LGB_pred_test = LGB.predict(X_test_only_numeric)
    
    # mae = mean_absolute_error(y_test, LGB_pred_test)
    # mse = mean_squared_error(y_test, LGB_pred_test)
    # rmse = np.sqrt(mse)
    # r2 = r2_score(y_test, LGB_pred_test)
    # metrics_LGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    
    # # Store information in tracking server
    # with mlflow.start_run(run_name = f"ts_into_features_Daily_LGB_{exchange}_{execution_date}") as run:
    #     #mlflow.log_params(params)
    #     mlflow.log_metrics(metrics_LGB)
    #     mlflow.sklearn.log_model(
    #         sk_model=LGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
    #     )
    # print(f"Run: {model} - {exchange}")
    


def train_daily(exchanges):

    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        mlflow_daily(exchange)


def main():

    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    train_daily(exchanges)
    
    
    
if __name__ == "__main__":
    main()
