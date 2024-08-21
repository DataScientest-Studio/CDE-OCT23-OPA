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
import json
from typing import List, Dict, Any

from outils.data_modeling_daily import get_cutoff_indices, transform_ts_data_into_features_and_target, train_test_split, ts_into_features_Daily, get_daily_data
from outils.ft_tables import load_data
from outils.functions import charge_model
from models.tsif_model_daily import mlflow_daily, mlflow_daily_register

def register_daily_model(exchange, model):
    model = None

    # IS THERE ANY MODEL? 
    try:
        model = charge_model(exchange, model)
    
    except Exception as e:
        print(f"{e}")
        return None
    
    # NO MODEL
    if model is None:
        
        print("There is no model")
        
        df_original = get_daily_data(exchange)
        #print(df_original)
        
        # NO DATA
        #if df_original == None:
        if df_original is None or len(df_original) < 1:
            
            #Load the data:
            load_data(exchange)
            print("Data loaded")
            
            #Train and register the model:
            mlflow_daily_register(exchange)
            return "Model registered"
        
        # DATA 
        else:
            #Train and register the model:
            mlflow_daily_register(exchange)
            

def predict_test_data(exchange, model_name):
    
    model = None

    # IS THERE ANY MODEL? 
    try:
        model = charge_model(exchange, model_name)
    
    
    except Exception as e:
        print(f"{e}")
        return None
    
    # NO MODEL
    if model is None:
        
        print("There is no model")
        
        df_original = get_daily_data(exchange)
        #print(df_original)
        
        # NO DATA
        #if df_original == None:
        if df_original is None or len(df_original) < 1:
            
            #Load the data:
            load_data(exchange)
            print("Data loaded")
            
            #Train and register the model:
            mlflow_daily_register(exchange)
            
            #Charge the model:
            model = charge_model(exchange, model_name)
            
            #Get the data to predict:
            X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
            
            #Predict
            predictions = model.predict(pd.DataFrame(X_test_only_numeric))
            return predictions 
        
        # DATA 
        else:
            #Train and register the model:
            mlflow_daily_register(exchange)
            
            #Charge the model:
            model = charge_model(exchange, model_name)
            
            #Get the data to predict:
            X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
            
            #Predict
            predictions = model.predict(pd.DataFrame(X_test_only_numeric))
            return predictions 
            
    # MODEL       
    else:
        
        print("There is a model registered")
        
        df_original = get_daily_data(exchange)
        
        # NO DATA
        if df_original is None or len(df_original) < 1:
            
            # Load the data:
            load_data(exchange)
            print("Data loaded")
            
            # Get data
            X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
            
            # Predict
            predictions = model.predict(pd.DataFrame(X_test_only_numeric))
            return predictions 
            
            
        # DATA
        else:
            
            # Get data
            X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
            
            # Predict
            predictions = model.predict(pd.DataFrame(X_test_only_numeric))
            return predictions
        
        
def return_test_prediction_data(exchange: str, model: str) -> dict:
    # Assuming these functions are defined elsewhere and imported
    X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
    predictions = predict_test_data(exchange, model)

    X_train = X_train[['datetime']]
    X_train['Open'] = y_train
    X_train['Exchange'] = exchange

    X_test = X_test[['datetime']]
    X_test['Open'] = y_test
    X_test['Exchange'] = exchange

    pred = X_test[['datetime']]
    pred['Open'] = predictions
    pred['Exchange'] = exchange

    train_test = pd.concat([X_train, X_test], ignore_index=True)

    # Convert DataFrames to JSON
    train_test_json = train_test.to_json(orient="records")
    pred_json = pred.to_json(orient="records")

    result = {
        "train_test": json.loads(train_test_json),
        "pred": json.loads(pred_json)
    }

    return result


def predict_exchange_future(exchange, model_name, days):

    df_original = get_daily_data(exchange)

    df = df_original[['id_date', 'Open', 'Exchange']]
    df['datetime'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df = df[['datetime', 'Open', 'Exchange']]

    features, targets = transform_ts_data_into_features_and_target(
        df,
        input_seq_len=31,  # one week of history -> 24*7*1
        step_size=1,
    )

    past_close_columns = [c for c in features.columns if c.startswith('open_')]

    features_only_numeric = features[past_close_columns]
    dates = features[['datetime']]

    df_result = pd.DataFrame()
    df_dates = pd.DataFrame()

    last_day = features_only_numeric.iloc[-1:]
    last_date = dates.iloc[-1:]

    model = charge_model(exchange, model_name)
    if model is None:
        print("There is no model")

    prediction = model.predict(last_day)

    df_result = pd.concat([df_result, last_day], ignore_index=True)
    df_result['predicted_price'] = prediction

    # Dates
    last_date_day = last_date.iloc[-1]
    df_dates = pd.concat([df_dates, last_date], ignore_index=True)
    day = last_date_day

    for i in range(1, days):
        last_row = df_result.iloc[-1]
        last_row = last_row.drop('predicted_price')

        new_row = last_row.shift(-1)
        new_row['open_previous_1_day'] = prediction
        new_row_df = pd.DataFrame([new_row], columns=df_result.columns)

        prediction = model.predict(new_row_df)
        new_row_df['predicted_price'] = prediction

        df_result = pd.concat([df_result, new_row_df], ignore_index=True)

        # Dates
        day = day + timedelta(days=1)
        new_row_date = pd.DataFrame([day], columns=df_dates.columns)
        df_dates = pd.concat([df_dates, new_row_date], ignore_index=True)

    result = pd.concat([df_result, df_dates], axis=1)
    result['exchange'] = exchange
    
    result = result[['datetime', 'exchange', 'predicted_price']]
    result['datetime'] = result['datetime'].dt.strftime('%Y-%m-%d')
    

    # Convert the result DataFrame to JSON
    result = result.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
    return result