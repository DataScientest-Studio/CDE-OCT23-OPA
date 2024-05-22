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
import os

# FUNCTIONS:

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


from tqdm import tqdm

def transform_ts_data_into_features_and_target(
    ts_data: pd.DataFrame,
    input_seq_len: int,
    step_size: int
) -> pd.DataFrame:
    """
    Slices and transposes data from time-series format into a (features, target)
    format that we can use to train Supervised ML models
    """
    assert set(ts_data.columns) == {'datetime', 'Open', 'Exchange'}

    exchanges = ts_data['Exchange'].unique()
    #print(exchanges)
    features = pd.DataFrame()
    targets = pd.DataFrame()
    
    for exchange in tqdm(exchanges):
        
        # keep only ts data for this `location_id`
        ts_data_one_exchange = ts_data.loc[
            ts_data.Exchange == exchange, 
            ['datetime', 'Open']
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
            x[i, :] = ts_data_one_exchange.iloc[idx[0]:idx[1]]['Open'].values
            y[i] = ts_data_one_exchange.iloc[idx[1]:idx[2]]['Open'].values
            hours.append(ts_data_one_exchange.iloc[idx[1]]['datetime'])


        # numpy -> pandas
        features_one_exchange = pd.DataFrame(
            x,
            columns=[f'open_previous_{i+1}_day' for i in reversed(range(input_seq_len))]
        )
        features_one_exchange['datetime'] = hours
        features_one_exchange['exchange'] = exchange

        # numpy -> pandas
        targets_one_exchange = pd.DataFrame(y, columns=[f'target_open_next_day'])

        # concatenate results
        features = pd.concat([features, features_one_exchange])
        targets = pd.concat([targets, targets_one_exchange])

    features.reset_index(inplace=True, drop=True)
    targets.reset_index(inplace=True, drop=True)

    return features, targets['target_open_next_day']


def train_test_split(
    df: pd.DataFrame,
    cutoff_date: datetime,
    target_column_name: str,
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    """
    """
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



# ----------------------------------------------------

def daily_models(exchange):
    temporality = 'daily'
    
    connection = mysql.connector.connect(
        user = 'root',
        password = 'root',
        host = 'localhost',
        port = 3306,
        database = 'Historical_Data'
    )
    print("MySQL DB Connected")
    
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_DAILY_DATA WHERE Exchange = '{exchange}'")

    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    df_original = pd.DataFrame(results, columns=columns)
    df = df_original[['id_date', 'Open','Exchange']]
    df['datetime'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df = df[['datetime', 'Open', 'Exchange']]

    features, targets = transform_ts_data_into_features_and_target(
        df,
        input_seq_len=31*12, # one week of history -> 24*7*1
        step_size=24,
    )
    
    df = pd.concat([features, targets],
               axis = 1)
    
    X_train, y_train, X_test, y_test = train_test_split(
        df,
        cutoff_date=datetime(2023, 5, 1, 0, 0, 0),
        target_column_name='target_open_next_day'
    )
    # use only past close data
    past_close_columns = [c for c in X_train.columns if c.startswith('open_')]
    X_train_only_numeric = X_train[past_close_columns]
    X_test_only_numeric = X_test[past_close_columns]

    # Initialize an empty DataFrame for metrics
    metrics_df = pd.DataFrame(columns=['Date', 'Exchange', 'Model', 'MSE', 'RMSE', 'MAE', 'R2', 'Temporality'])
    
    # Get the current date
    execution_date = datetime.now().strftime('%Y-%m-%d')

    # Function to add metrics to the DataFrame
    def add_metrics_to_df(model_name, metrics):
        nonlocal metrics_df
        model_name = f"{model_name}_2"
        temp_df = pd.DataFrame({
            'Date': [execution_date],
            'Exchange': [exchange],
            'Model': [model_name],
            'MSE': [metrics[0]],
            'RMSE': [metrics[1]],
            'MAE': [metrics[2]],
            'R2': [metrics[3]],
            'Temporality': [temporality]
        })
        metrics_df = pd.concat([metrics_df, temp_df], ignore_index=True)
        
        

    # Linear Regression
    regressor = LinearRegression()
    regressor.fit(X_train_only_numeric, y_train)
    regressor_pred_test = regressor.predict(X_test_only_numeric)
    reg_metrics = metrics_scikit_learn(y_test, regressor_pred_test)
    add_metrics_to_df('LinearRegression', reg_metrics)


    # XGBoost
    XGB = xgb.XGBRegressor()
    XGB.fit(X_train_only_numeric, y_train)
    XGB_pred_test = XGB.predict(X_test_only_numeric)
    XGB_metrics = metrics_scikit_learn(y_test, XGB_pred_test)
    add_metrics_to_df('XGBoost', XGB_metrics)

    
    # LightGBM
    LGB = lgb.LGBMRegressor()
    LGB.fit(X_train_only_numeric, y_train)
    LGB_pred_test = LGB.predict(X_test_only_numeric)
    LGB_metrics = metrics_scikit_learn(y_test, LGB_pred_test)
    add_metrics_to_df('LightGBM', LGB_metrics)
    
    
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.plot(X_train['datetime'], y_train, label='Reality - Train')
    ax.plot(X_test['datetime'], y_test, '-g', label='Reality - Test')
    ax.plot(X_test['datetime'], LGB_pred_test, ':b', label='LGB_Prediction')
    ax.plot(X_test['datetime'], XGB_pred_test, ':g', label='XGB_Prediction')
    ax.plot(X_test['datetime'], regressor_pred_test, ':r', label='LinearRegression_Prediction')
    ax.legend()
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.title('Prediction vs. Reality')
    
    # Create a directory to save plots if it doesn't exist
    if not os.path.exists('dataviz/daily'):
        os.makedirs('dataviz/daily')
        
    plot_filename = f'dataviz/daily/{exchange}_{execution_date}_2.png'

    # Check if the file already exists
    if os.path.exists(plot_filename):
        os.remove(plot_filename)  # Delete the existing file

    plt.savefig(plot_filename)  # Save the plot, overwriting any existing file with the same name
    plt.close(fig)
    
    
    return metrics_df

def insert_into_db(metrics_df, connection):
    cursor = connection.cursor()
    
    
    
    delete_query = """
    DELETE FROM Models_Results
    WHERE Date = %s AND Exchange = %s AND Model = %s AND Temporality = %s
    """
    
    metrics_df_insert_query = """
    INSERT INTO Models_Results (Date, Exchange, Model, MSE, RMSE, MAE, R2, Temporality)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    #Insert data
    cursor.execute("""SET FOREIGN_KEY_CHECKS = 0""")
    for index, row in metrics_df.iterrows():
        # Delete existing records with the same date, exchange, and model
        cursor.execute(delete_query, (row['Date'], row['Exchange'], row['Model'], row['Temporality']))
        
        # Insert the new record
        cursor.execute(metrics_df_insert_query, tuple(row))
        
    connection.commit()
    print("Metrics inserted correctly on the Models_Results table.")
        

def main(exchanges):
    
    
    connection = mysql.connector.connect(
    user='root',
    password='root',
    host='localhost',
    port=3306,
    database='ML'
    )
    print("MySQL to ML DB Connected")
    
    
    # Initialize an empty DataFrame to store all results
    all_metrics_df = pd.DataFrame()

    # Iterate over each exchange and collect the metrics
    for exchange in exchanges:
        metrics_df = daily_models(exchange)
        all_metrics_df = pd.concat([all_metrics_df, metrics_df], ignore_index=True)
    
    
    print(all_metrics_df)
    insert_into_db(all_metrics_df, connection)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some exchanges.')
    parser.add_argument('exchanges', type=str, help='Comma-separated list of exchanges')
    
    args = parser.parse_args()
    exchanges = args.exchanges.split(',')

    main(exchanges)
