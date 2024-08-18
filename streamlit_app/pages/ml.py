import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import io

# Streamlit UI
st.title("Machine learning:")

st.write(
    """
    - _If you want to train a new model, please download the latest data before_
    - If you want to continue predicting with the current model, not necessary to train another model. Only upload the data
    """
)


col1, col2 = st.columns(2)
with col1:
    exchange = st.text_input("Enter Exchange Symbol", "BTC-USD")

    if st.button("Load data"):
        with st.spinner("Drawing the graph..."):
            if load_data(exchange):
                st.write("Data loaded. Please try fetching the data again.")
                
with col2:
    st.write("\n")
    st.write("\n")
    st.page_link("pages/predictions.py", label="Predictions", icon=":material/waterfall_chart:")


# API endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
EXCHANGES_NUMBER = f"{FASTAPI_URL}/get_exchanges_number"
EXCHANGES_LAST_DATE = f"{FASTAPI_URL}/get_unique_exchanges"

LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
METRICS = f"{FASTAPI_URL}/metrics"


# Function to load data from the FastAPI endpoint
def load_data(exchange):
    response = requests.post(LOAD_DATA_ENDPOINT, json={"exchange": exchange})
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to load data")
        return None

# Function to fetch data from the FastAPI endpoint
def exchanges_number():
    url = f"{EXCHANGES_NUMBER}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to fetch data")
        return None
    
# Function to fetch data from the FastAPI endpoint
def exchanges_last_date():
    url = f"{EXCHANGES_LAST_DATE}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(eval(data))
        df['Last_Date'] = pd.to_datetime(df['Last_Date'], format='%Y%m%d').dt.strftime('%d-%m-%Y')
        df = df.sort_values(by='Last_Date', ascending =False)
        return df
    else:
        return None
    
def get_metrics():
    url = METRICS
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Error getting the metrics")
        return None
    
# MODELS
st.subheader("Models", divider = True)
data = get_metrics()

metrics_df = pd.DataFrame.from_dict(data)
        
# Rename columns
metrics_df.rename(columns={
    "run_id": "Run ID",
    "metrics.mse": "MSE",
    "metrics.mae": "MAE",
    "metrics.r2": "R2",
    "metrics.rmse": "RMSE",
    "tags.exchange": "Exchange",
    "tags.execution_date": "Execution Date",
    "tags.model_type": "Model"
}, inplace=True)
        
# Convert float columns to int
float_columns = ["MSE", "MAE", "RMSE"]
metrics_df[float_columns] = metrics_df[float_columns].astype(int)
        
# Convert 'Execution Date' to datetime
metrics_df['Execution Date'] = pd.to_datetime(metrics_df['Execution Date'])
metrics_df['Execution Date'] = pd.to_datetime( metrics_df['Execution Date'], format='%Y%m%d').dt.strftime('%d-%m-%Y')
metrics_df = metrics_df.sort_values(by=['R2'], ascending =False)
metrics_df = metrics_df[metrics_df['R2'] != 1]
        
desired_columns = ["Exchange", "Model", "Execution Date", "R2"]
metrics_df = metrics_df[desired_columns]
metrics_df = metrics_df.head(5)
        
metrics_df = metrics_df.drop_duplicates()
        
number_of_models = len(metrics_df)

# EXCHANGES
col1, col2 = st.columns(2)

with col1:
    st.metric(label="Number of models", value=f"{number_of_models}" if number_of_models is not None else "N/A")
    st.write("More:")
    st.page_link("pages/ml_models_metrics.py", label="ML Models Metrics", icon=":material/analytics:")

    st.page_link("pages/ml_models.py", label="ML Models", icon=":material/schema:")

    st.page_link("pages/predictions.py", label="Predictions", icon=":material/waterfall_chart:")

    
with col2:
    st.markdown(metrics_df.style.hide(axis="index").to_html(), unsafe_allow_html = True)
    #st.dataframe(data)



