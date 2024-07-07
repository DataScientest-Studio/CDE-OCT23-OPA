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


# Functions:

def get_cutoff_indices(
    data: pd.DataFrame,
    n_features: int, 
    step_size:int
) -> list:
    
    stop_position = len(data) - 1
    
    subseq_first_idex = 0
    subseq_mid_idx = n_features
    subseq_last_idx = n_features + 1
    indices = []
    
    while subseq_last_idx <= stop_position:
        indices.append((subseq_first_idex, subseq_mid_idx, subseq_last_idx))
        
        subseq_first_idex += step_size
        subseq_mid_idx += step_size
        subseq_last_idx += step_size
        
    return indices


def transform_ts_data_into_features_and_target(
    ts_data: pd.DataFrame,
    input_seq_len: int,
    step_size: int
) -> pd.DataFrame:
    """
    Slices and transposes data from time-series format into a (features, target)
    format that we can use to train Supervised ML models
    """
    assert set(ts_data.columns) == {'datetime', 'Close', 'Exchange'}

    exchanges = ts_data['Exchange'].unique()
    #print(exchanges)
    features = pd.DataFrame()
    targets = pd.DataFrame()
    
    for exchange in tqdm(exchanges):
        
        # keep only ts data for this `location_id`
        ts_data_one_exchange = ts_data.loc[
            ts_data.Exchange == exchange, 
            ['datetime', 'Close']
        ]

        # pre-compute cutoff indices to split dataframe rows
        indices = get_cutoff_indices(
            ts_data_one_exchange,
            input_seq_len,
            step_size
        )

        # slice and transpose data into numpy arrays for features and targets
        n_examples = len(indices)
        x = np.ndarray(shape=(n_examples, input_seq_len), dtype=np.float32)
        y = np.ndarray(shape=(n_examples), dtype=np.float32)
        hours = []
        
        for i, idx in enumerate(indices):
            x[i, :] = ts_data_one_exchange.iloc[idx[0]:idx[1]]['Close'].values
            y[i] = ts_data_one_exchange.iloc[idx[1]:idx[2]]['Close'].values
            hours.append(ts_data_one_exchange.iloc[idx[1]]['datetime'])


        # numpy -> pandas
        features_one_exchange = pd.DataFrame(
            x,
            columns=[f'close_previous_{i+1}_hour' for i in reversed(range(input_seq_len))]
        )
        features_one_exchange['datetime'] = hours
        features_one_exchange['exchange'] = exchange

        # numpy -> pandas
        targets_one_exchange = pd.DataFrame(y, columns=[f'target_close_next_hour'])

        # concatenate results
        features = pd.concat([features, features_one_exchange])
        targets = pd.concat([targets, targets_one_exchange])

    features.reset_index(inplace=True, drop=True)
    targets.reset_index(inplace=True, drop=True)

    return features, targets['target_close_next_hour']


def train_test_split(
    df: pd.DataFrame,
    cutoff_date: datetime,
    target_column_name: str,
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:

    train_data = df[df.datetime < cutoff_date].reset_index(drop=True)
    test_data = df[df.datetime >= cutoff_date].reset_index(drop=True)

    X_train = train_data.drop(columns=[target_column_name])
    y_train = train_data[target_column_name]
    X_test = test_data.drop(columns=[target_column_name])
    y_test = test_data[target_column_name]

    return X_train, y_train, X_test, y_test

def metrics_scikit_learn(y_test, predictions):
    mse = mean_squared_error(y_test, predictions)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    return mse, rmse, mae, r2


# ------------------------------------------------------

def ts_into_features_hourly_LR(exchange):
    temporality = 'hour'
    
    connection = mysql.connector.connect(
        user='root',
        password='root',
        host='localhost',
        port=3306,
        database='Historical_Data'
    )
    print("MySQL DB Connected")
    
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_HOUR_DATA WHERE Exchange = '{exchange}'")

    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    df_original = pd.DataFrame(results, columns=columns)
    df = df_original[['id_date', 'Hour', 'Close','Exchange']]

    df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df['datetime'] = df['id_date'] + pd.to_timedelta(df['Hour'], unit='h')

    df = df[['datetime', 'Close', 'Exchange']]
    
    features, targets = transform_ts_data_into_features_and_target(
    df,
    input_seq_len=24, # one week of history -> 24*7*1
    step_size=24,
    )
    
    df = pd.concat([features, targets],
               axis = 1)
    
    X_train, y_train, X_test, y_test = train_test_split(
        df,
        cutoff_date=datetime(2024, 4, 1, 0, 0, 0),
        target_column_name='target_close_next_hour'
    )
    
    # use only past close data
    past_close_columns = [c for c in X_train.columns if c.startswith('close_')]
    X_train_only_numeric = X_train[past_close_columns]
    X_test_only_numeric = X_test[past_close_columns]
    
    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')
    
    # Define tracking_uri
    client = MlflowClient(tracking_uri="http://127.0.0.1:8080")
    
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment(f"ts_into_features_Hourly_LR_{exchange}")
    #run_name = "second_run"
    artifact_path = f"ts_into_features_Hourly_LR_{exchange}"
    
    # Linear Regression
    model = 'ts_into_features_Hourly_LR'
    LR = LinearRegression()
    LR.fit(X_train_only_numeric, y_train)
    regressor_pred_test = LR.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, regressor_pred_test)
    mse = mean_squared_error(y_test, regressor_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, regressor_pred_test)
    metrics_LR = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    # Store information in tracking server
    with mlflow.start_run(run_name = f"ts_into_features_Hourly_LR_{exchange}_{execution_date}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics_LR)
        mlflow.sklearn.log_model(
            sk_model=LR, input_example=X_test_only_numeric, artifact_path=artifact_path
        )
    print(f"Run: {model} - {exchange}")
    
    #XGBOOST
        
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment(f"ts_into_features_Hourly_XGB_{exchange}")
    #run_name = "second_run"
    artifact_path_XGB = f"ts_into_features_Hourly_XGB_{exchange}"
    
    
    # Linear Regression
    model = 'ts_into_features_Hourly_XGB'
    XGB = xgb.XGBRegressor()
    XGB.fit(X_train_only_numeric, y_train)
    XGB_pred_test = XGB.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, XGB_pred_test)
    mse = mean_squared_error(y_test, XGB_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, XGB_pred_test)
    metrics_XGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    
    # Store information in tracking server
    with mlflow.start_run(run_name = f"ts_into_features_Daily_XGB_{exchange}_{execution_date}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics_XGB)
        mlflow.sklearn.log_model(
            sk_model=XGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
        )
        
    print(f"Run: {model} - {exchange}") 
    
    #LGB
        
    # Define experiment name, run name and artifact_path name
    apple_experiment = mlflow.set_experiment(f"ts_into_features_Hourly_LGB_{exchange}")
    #run_name = "second_run"
    artifact_path_XGB = f"ts_into_features_Hourly_LGB_{exchange}"
    
    
    # Linear Regression
    model = 'ts_into_features_Hourly_LGB'
    LGB = lgb.LGBMRegressor()
    LGB.fit(X_train_only_numeric, y_train)
    LGB_pred_test = LGB.predict(X_test_only_numeric)
    
    mae = mean_absolute_error(y_test, LGB_pred_test)
    mse = mean_squared_error(y_test, LGB_pred_test)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, LGB_pred_test)
    metrics_LGB = {"mae": mae, "mse": mse, "rmse": rmse, "r2": r2}
    
    
    # Store information in tracking server
    with mlflow.start_run(run_name = f"ts_into_features_Hourly_LGB_{exchange}_{execution_date}") as run:
        #mlflow.log_params(params)
        mlflow.log_metrics(metrics_LGB)
        mlflow.sklearn.log_model(
            sk_model=LGB, input_example=X_test_only_numeric, artifact_path=artifact_path_XGB
        )
    print(f"Run: {model} - {exchange}")
      
    
     

def main(exchanges):
    
    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        ts_into_features_hourly_LR(exchange)

        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    main(exchanges)
