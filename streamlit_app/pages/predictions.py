import streamlit as st
import requests
import pandas as pd
import plotly.graph_objs as go

# API endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
TEST_PREDICTIONS = f"{FASTAPI_URL}/return_test_prediction_data"
FUTURE_PREDICTIONS = f"{FASTAPI_URL}/predict_exchange_future"

# Streamlit UI
st.title("ML: Predictions")
st.subheader("The different graphics are:")
st.markdown("""
- Train and test comparaison
- Future prediction
""")

st.subheader("The train - test comparaison:", divider = True)

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
 
def get_future_predictions(exchange, model, days=30):
    url = f"{FUTURE_PREDICTIONS}/{exchange}&{model}&{days}"
    response = requests.post(url)
    if response.status_code == 200:
        data = response.json()
        return data
    
    else:
        st.error("Error getting the metrics")
        return None, None






# Streamlit input and visualization
with st.container():
    col1, col2 = st.columns([1, 1])
    
    with col1:
        exchange = st.text_input('Enter the exchange code', 'BTC-USD')
        
    with col2:
        model_name = st.selectbox(
            "Select Model",
            ("LinearRegression", "XGBoost", "RandomForestRegressor", "DecisionTreeRegressor")
        )


if 'exchange' not in st.session_state:
    st.session_state.exchange = "BTC-USD"

if 'model_name' not in st.session_state:
    st.session_state.model_name = "LinearRegression"
    
# Update session state when the text input changes
if exchange != st.session_state.exchange:
    st.session_state.exchange = exchange
    
if st.button("Plot"):
    if exchange and model_name:
        plot_predictions_test(exchange, model_name)
