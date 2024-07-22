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

from functions_daily import get_cutoff_indices, transform_ts_data_into_features_and_target, train_test_split, ts_into_features_Daily, get_data
from ft_tables import load_data
from functions import charge_model
from train_tsif_model_daily import mlflow_daily, mlflow_daily_register



def predict_test_data(exchange):
    
    model = None

    # IS THERE ANY MODEL? 
    try:
        model = charge_model(exchange)
    
    
    except Exception as e:
        print(f"{e}")
        return None
    
    # NO MODEL
    if model is None:
        
        print("There is no model")
        
        df_original = get_data(exchange)
        #print(df_original)
        
        # NO DATA
        #if df_original == None:
        if len(df_original) < 1:
            
            #Load the data:
            load_data(exchange)
            print("Data loaded")
            
            #Train and register the model:
            mlflow_daily_register(exchange)
            
            #Charge the model:
            model = charge_model(exchange)
            
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
            model = charge_model(exchange)
            
            #Get the data to predict:
            X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)
            
            #Predict
            predictions = model.predict(pd.DataFrame(X_test_only_numeric))
            return predictions 
            
    # MODEL       
    else:
        
        print("There is a model registered")
        
        df_original = get_data(exchange)
        
        # NO DATA
        if len(df_original) < 1:
            
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


def plot_predictions_test(exchange):

    #model = charge_model(exchange)
    
    predictions = predict_test_data(exchange)

    X_test_only_numeric, X_train_only_numeric, y_test, y_train, X_train, X_test = ts_into_features_Daily(exchange)

    #historical_data = 

    X_train = X_train[['datetime']]
    X_train['price'] = y_train
    X_train

    X_test = X_test[['datetime']]
    X_test['price'] = y_test
    X_test

    pred = X_test[['datetime']]
    pred['price'] = predictions


    concatenated = pd.concat([X_train, X_test], ignore_index = True)
    
    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(concatenated['datetime'], concatenated['price'], label='Actual Price', color='blue')
    plt.plot(pred['datetime'], pred['price'], label='Predicted Price', color='red', linestyle='--')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Actual vs Predicted Prices')
    plt.legend()
    plt.grid(True)
    plt.show()
    
    
