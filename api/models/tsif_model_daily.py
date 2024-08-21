import warnings
warnings.filterwarnings("ignore")

from mlflow import MlflowClient, set_tracking_uri
import mlflow
from mlflow.models import infer_signature

from typing import Tuple
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
import pyarrow
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import argparse
import os
from dateutil.relativedelta import relativedelta


from outils.data_modeling_daily import get_cutoff_indices, transform_ts_data_into_features_and_target, train_test_split, ts_into_features_Daily, get_daily_data
from outils.ft_tables import load_data, load_daily_data

def mlflow_daily_register(exchange, model):
    #Recharge the data of the exchange and retrain the model
    load_daily_data(exchange)
    
    df_original = get_daily_data(exchange)
    
    if len(df_original) < 1:
        return("No data in the database")
    
    else:
        
        X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
        
        
        execution_date = datetime.now().strftime('%Y-%m-%d')
        # Define tracking_uri to point to the MLflow server in Docker
        #client = MlflowClient(tracking_uri="http://localhost:5000")
        set_tracking_uri("http://localhost:5000") 
        
        

        
        if model == 'LinearRegression':
            
            # Define experiment name, run name and artifact_path name
            apple_experiment = mlflow.set_experiment(f"Daily_{exchange}_LinearRegression")
            #run_name = "second_run"
            artifact_path_LR = f"Daily_{exchange}_LinearRegression"
            
            # Linear Regression
            model = 'LinearRegression'
            LR = LinearRegression()
            LR.fit(X_train_only_numeric, y_train)
            regressor_pred_test = LR.predict(X_test_only_numeric)
            
            mae = mean_absolute_error(y_test, regressor_pred_test)
            mse = mean_squared_error(y_test, regressor_pred_test)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, regressor_pred_test)
            metrics_LR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
            
            signature = infer_signature(X_test_only_numeric, regressor_pred_test)
            
            
            # Store information in tracking server
            with mlflow.start_run(run_name = f"ts_into_features_Daily_LR_{exchange}_{execution_date}") as run:
                #mlflow.log_params(params)
                mlflow.log_metrics(metrics_LR)
                mlflow.sklearn.log_model(
                    sk_model=LR, input_example=X_test_only_numeric, artifact_path=artifact_path_LR,
                    signature = signature,
                    registered_model_name = f"{exchange}_Daily_Model_{model}"
                )

                mlflow.set_tag("model_type", "Linear Regression")
                mlflow.set_tag("exchange", exchange)
                mlflow.set_tag("execution_date", execution_date)
                
            print(f"Run: {model} - {exchange}")
        
        # #XGBOOST
        elif model == 'XGBoost':

            
            # Define experiment name, run name and artifact_path name
            apple_experiment = mlflow.set_experiment(f"Daily_{exchange}_XGBoost")
            #run_name = "second_run"
            artifact_path_XGB = f"Daily_{exchange}_XGBoost"
            
            
            # Linear Regression
            model = 'XGBoost'
            XGB = xgb.XGBRegressor()
            XGB.fit(X_train_only_numeric, y_train)
            XGB_pred_test = XGB.predict(X_test_only_numeric)
            
            mae = mean_absolute_error(y_test, XGB_pred_test)
            mse = mean_squared_error(y_test, XGB_pred_test)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, XGB_pred_test)
            metrics_XGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
            
            signature = infer_signature(X_test_only_numeric, XGB_pred_test)
            
            
            # Store information in tracking server
            with mlflow.start_run(run_name = f"ts_into_features_Daily_XGB_{exchange}_{execution_date}") as run:
                #mlflow.log_params(params)
                mlflow.log_metrics(metrics_XGB)
                mlflow.sklearn.log_model(
                    sk_model=XGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB,
                    signature = signature,
                    registered_model_name = f"{exchange}_Daily_Model_{model}"
                )

                mlflow.set_tag("model_type", "XGB")
                mlflow.set_tag("exchange", exchange)
                mlflow.set_tag("execution_date", execution_date)
                
            print(f"Run: {model} - {exchange}")
            
        # RandomForestRegressor
        elif model == 'RandomForestRegressor':

            
            # Define experiment name, run name and artifact_path name
            apple_experiment = mlflow.set_experiment(f"Daily_{exchange}_RandomForestRegressor")
            #run_name = "second_run"
            artifact_path_RFR = f"Daily_{exchange}_RandomForestRegressor"
            
            
            # Linear Regression
            model = 'RandomForestRegressor'
            RFR = RandomForestRegressor()
            RFR.fit(X_train_only_numeric, y_train)
            RFR_pred_test = RFR.predict(X_test_only_numeric)
            
            mae = mean_absolute_error(y_test, RFR_pred_test)
            mse = mean_squared_error(y_test, RFR_pred_test)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, RFR_pred_test)
            metrics_RFR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
            
            signature = infer_signature(X_test_only_numeric, RFR_pred_test)
            
            
            # Store information in tracking server
            with mlflow.start_run(run_name = f"ts_into_features_Daily_RFR_{exchange}_{execution_date}") as run:
                #mlflow.log_params(params)
                mlflow.log_metrics(metrics_RFR)
                mlflow.sklearn.log_model(
                    sk_model=RFR, input_example=X_test_only_numeric, artifact_path=artifact_path_RFR,
                    signature = signature,
                    registered_model_name = f"{exchange}_Daily_Model_{model}"
                )

                mlflow.set_tag("model_type", "RandomForestRegressor")
                mlflow.set_tag("exchange", exchange)
                mlflow.set_tag("execution_date", execution_date)
                
            print(f"Run: {model} - {exchange}")
            
        # DecisionTreeRegressor
        elif model == 'DecisionTreeRegressor':

            
            # Define experiment name, run name and artifact_path name
            apple_experiment = mlflow.set_experiment(f"Daily_{exchange}_DecisionTreeRegressor")
            #run_name = "second_run"
            artifact_path_DTR = f"Daily_{exchange}_DecisionTreeRegressor"
            
            
            # Linear Regression
            model = 'DecisionTreeRegressor'
            DTR = DecisionTreeRegressor()
            DTR.fit(X_train_only_numeric, y_train)
            DTR_pred_test = DTR.predict(X_test_only_numeric)
            
            mae = mean_absolute_error(y_test, DTR_pred_test)
            mse = mean_squared_error(y_test, DTR_pred_test)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, DTR_pred_test)
            metrics_DTR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
            
            signature = infer_signature(X_test_only_numeric, DTR_pred_test)
            
            
            # Store information in tracking server
            with mlflow.start_run(run_name = f"ts_into_features_Daily_DTR_{exchange}_{execution_date}") as run:
                #mlflow.log_params(params)
                mlflow.log_metrics(metrics_DTR)
                mlflow.sklearn.log_model(
                    sk_model=DTR, input_example=X_test_only_numeric, artifact_path=artifact_path_DTR,
                    signature = signature,
                    registered_model_name = f"{exchange}_Daily_Model_{model}"
                )

                mlflow.set_tag("model_type", "DecisionTreeRegressor")
                mlflow.set_tag("exchange", exchange)
                mlflow.set_tag("execution_date", execution_date)
                
                
            client = MlflowClient()
            
            print(f"Run: {model} - {exchange}")
            





def mlflow_daily(exchange):
    
    data_loaded = False
    try:
        X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
        data_loaded = True
        
    except Exception as e:
        return(f"Error loading data: {e}")
        
    if data_loaded:
        
        execution_date = datetime.now().strftime('%Y-%m-%d')
        # Define tracking_uri to point to the MLflow server in Docker
        #client = MlflowClient(tracking_uri="http://localhost:5000")
        set_tracking_uri("http://localhost:5000") 
        
        
        # Define experiment name, run name and artifact_path name
        apple_experiment = mlflow.set_experiment(f"ts_into_features_Daily_{exchange}")
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
            mlflow.set_tag("model_type", "Linear Regression")
            mlflow.set_tag("exchange", exchange)
            mlflow.set_tag("execution_date", execution_date)
            
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
