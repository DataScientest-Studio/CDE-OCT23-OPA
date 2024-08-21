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
from outils.functions import database_connection

# Functions:



def get_hourly_data(exchange):
    
    connection = database_connection()    
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_HOUR_DATA WHERE Exchange = '{exchange}'")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
            
    df_original = pd.DataFrame(results, columns=columns)
    
    if df_original.empty:
        print("There is no data")
        return None
        
    else:
        return df_original
    

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

# ------------------------------------------------------

    
def ts_into_features_hourly(exchange):
    temporality = 'hour'
    
    df_original = get_hourly_data(exchange)
    
    df = df_original[['id_date', 'Hour', 'Close','Exchange']]

    df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df['datetime'] = df['id_date'] + pd.to_timedelta(df['Hour'], unit='h')

    df = df[['datetime', 'Close', 'Exchange']]
    
    features, targets = transform_ts_data_into_features_and_target(
    df,
    input_seq_len=24*7*1, # one week of history -> 24*7*1
    step_size=24,
    )
    
    df = pd.concat([features, targets],
               axis = 1)
    

    # Calculate the cutoff_date as the first day of 6 months ago
    cutoff_date = (datetime.now() - relativedelta(months=1)).replace(day=1)


    # Use the provided train_test_split function
    X_train, y_train, X_test, y_test = train_test_split(
        df,
        cutoff_date=cutoff_date,
        target_column_name='target_close_next_hour'
    )
        
    # use only past close data
    past_close_columns = [c for c in X_train.columns if c.startswith('close_')]
    X_train_only_numeric = X_train[past_close_columns]
    X_test_only_numeric = X_test[past_close_columns]
    
    return X_test_only_numeric, X_train_only_numeric, y_test, y_train