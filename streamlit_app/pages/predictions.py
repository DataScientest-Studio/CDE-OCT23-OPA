import streamlit as st
import requests
import pandas as pd
import plotly.graph_objs as go

# API endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
TEST_PREDICTIONS = f"{FASTAPI_URL}/return_test_prediction_data"
FUTURE_PREDICTIONS = f"{FASTAPI_URL}/predict_exchange_future"

# TRAIN - TEST 
def get_test_predictions(exchange, model):
    url = f"{TEST_PREDICTIONS}/{exchange}&{model}"
    response = requests.post(url)
    if response.status_code == 200:
        data = response.json()
        df_reality = pd.DataFrame(data['data']['train_test'])
        df_pred = pd.DataFrame(data['data']['pred'])
        
        # Convert the 'datetime' column from milliseconds to datetime objects
        df_reality['datetime'] = pd.to_datetime(df_reality['datetime'], unit='ms')
        df_pred['datetime'] = pd.to_datetime(df_pred['datetime'], unit='ms')
    
        return df_reality, df_pred
    else:
        st.error("Error getting the metrics")
        return None, None

def plot_predictions_test(exchange, model):
    df_reality, df_pred = get_test_predictions(exchange, model)
    
    if df_reality is None or df_pred is None:
        return
    
    # Create traces for actual and predicted prices
    actual_trace = go.Scatter(
        x=df_reality['datetime'], 
        y=df_reality['Open'],
        mode='lines',
        name='Actual Price',
        line=dict(color='blue')
    )
    
    predicted_trace = go.Scatter(
        x=df_pred['datetime'], 
        y=df_pred['Open'],
        mode='lines',
        name='Predicted Price',
        line=dict(color='red', dash='dash')
    )
    
    # Layout
    layout = go.Layout(
        title='Actual vs Predicted Prices',
        xaxis=dict(title='Date'),
        yaxis=dict(title='Price'),
        showlegend=True
    )
    
    # Create the figure and plot it
    fig = go.Figure(data=[actual_trace, predicted_trace], layout=layout)
    
    st.plotly_chart(fig)

# FUTURE
def get_future_predictions(exchange, model):
    url = f"{FUTURE_PREDICTIONS}/{exchange}&{model}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data = pd.DataFrame(data['data'])
        return data
    
    else:
        st.error("Error getting the metrics")
        return None

def plot_future_prediction(exchange, model):
    df_reality, df_pred = get_test_predictions(exchange, model)
    
    if df_reality is None or df_pred is None:
        return
    
    df_future = get_future_predictions(exchange, model)
    
    if df_future is None:
        return

    # Convert 'datetime' columns to datetime objects if they're not already
    df_reality['datetime'] = pd.to_datetime(df_reality['datetime'])
    df_future['datetime'] = pd.to_datetime(df_future['datetime'])
    
    # Get the last date in the actual data
    last_date = df_reality['datetime'].max()

    # Filter the data for the last month of actual prices
    start_last_month = last_date - pd.DateOffset(months=1)
    df_reality_last_month = df_reality[df_reality['datetime'] >= start_last_month]

    # Get the start date for the predicted future data
    start_future = last_date + pd.DateOffset(days=1)
    
    # Filter the future data for the next month
    end_future = start_future + pd.DateOffset(months=1)
    df_future_next_month = df_future[(df_future['datetime'] >= start_future) & (df_future['datetime'] < end_future)]

    # Create traces for actual and predicted prices
    actual_trace = go.Scatter(
        x=df_reality_last_month['datetime'], 
        y=df_reality_last_month['Open'],
        mode='lines',
        name='Actual Price',
        line=dict(color='blue')
    )
    
    predicted_trace = go.Scatter(
        x=df_future_next_month['datetime'], 
        y=df_future_next_month['predicted_price'],
        mode='lines',
        name='Predicted Price',
        line=dict(color='red', dash='dash')
    )
    
    # Layout
    layout = go.Layout(
        title='Actual Prices (Last Month) and Predicted Prices (Next Month)',
        xaxis=dict(title='Date'),
        yaxis=dict(title='Price'),
        showlegend=True
    )
    
    # Create the figure and plot it
    fig = go.Figure(data=[actual_trace, predicted_trace], layout=layout)
    
    st.plotly_chart(fig)

# Streamlit UI
st.title("ML: Predictions")
st.subheader("The different graphics are:")
st.markdown("""
- Train and test comparison
- Future prediction
""")

st.subheader("The train - test comparison:", divider=True)

with st.container():
    col1, col2 = st.columns([1, 1])
    
    with col1:
        exchange = st.text_input('Enter the exchange code', 'BTC-USD')
        
    with col2:
        model_name = st.selectbox(
            "Select Model",
            ("LinearRegression", "XGBoost", "RandomForestRegressor", "DecisionTreeRegressor")
        )

    # Initialize session state variables
    if 'exchange' not in st.session_state:
        st.session_state.exchange = exchange
    
    if 'model_name' not in st.session_state:
        st.session_state.model_name = model_name

    # Update session state when the text input changes
    if exchange != st.session_state.exchange:
        st.session_state.exchange = exchange
    
    if model_name != st.session_state.model_name:
        st.session_state.model_name = model_name

if st.button("Plot"):
    if st.session_state.exchange and st.session_state.model_name:
        with st.spinner("Drawing the graph..."):
            plot_predictions_test(st.session_state.exchange, st.session_state.model_name)

st.subheader("The future 30 days prediction:", divider=True)

if st.button("Plot future predictions"):
    if st.session_state.exchange and st.session_state.model_name:
        with st.spinner("Drawing the graph..."):
            plot_future_prediction(st.session_state.exchange, st.session_state.model_name)
