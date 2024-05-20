import warnings
warnings.filterwarnings("ignore")
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
import argparse

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

def metrics_scikit_learn(y_test, predictions):
    mse = mean_squared_error(y_test, predictions)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    return mse, rmse, mae, r2

def daily_models(exchange):
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

    X_train = X_train.apply(lambda col: col.astype(int))
    X_test = X_test.apply(lambda col: col.astype(int))
    y_train = y_train.astype(int)
    y_test = y_test.astype(int)

    # Initialize an empty DataFrame for metrics
    metrics_df = pd.DataFrame(columns=['Date', 'Exchange', 'Model', 'MSE', 'RMSE', 'MAE', 'R2'])

    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')

    # Function to add metrics to the DataFrame
    def add_metrics_to_df(model_name, metrics):
        nonlocal metrics_df
        temp_df = pd.DataFrame({
            'Date': [execution_date],
            'Exchange': [exchange],
            'Model': [model_name],
            'MSE': [metrics[0]],
            'RMSE': [metrics[1]],
            'MAE': [metrics[2]],
            'R2': [metrics[3]]
        })
        metrics_df = pd.concat([metrics_df, temp_df], ignore_index=True)

    # Linear Regression
    regressor = LinearRegression()
    regressor.fit(X_train, y_train)
    regressor_pred_test = regressor.predict(X_test)
    reg_metrics = metrics_scikit_learn(y_test, regressor_pred_test)
    add_metrics_to_df('LinearRegression', reg_metrics)

    # XGBoost
    XGB = xgb.XGBRegressor()
    XGB.fit(X_train, y_train)
    XGB_pred_test = XGB.predict(X_test)
    XGB_metrics = metrics_scikit_learn(y_test, XGB_pred_test)
    add_metrics_to_df('XGBoost', XGB_metrics)

    # LightGBM
    LGB = lgb.LGBMRegressor()
    LGB.fit(X_train, y_train)
    LGB_pred_test = LGB.predict(X_test)
    LGB_metrics = metrics_scikit_learn(y_test, LGB_pred_test)
    add_metrics_to_df('LightGBM', LGB_metrics)

    return metrics_df

def main(exchanges):
    # Initialize an empty DataFrame to store all results
    all_metrics_df = pd.DataFrame()

    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        metrics_df = daily_models(exchange)
        all_metrics_df = pd.concat([all_metrics_df, metrics_df], ignore_index=True)

    print(all_metrics_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    main(exchanges)
