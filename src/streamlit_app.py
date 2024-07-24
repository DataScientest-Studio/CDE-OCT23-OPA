import streamlit as st
import requests

# Define the FastAPI endpoint
FASTAPI_URL = "http://127.0.0.1:8000/load_data"

# Streamlit app
st.title("Exchange Data Loader")

# Input for the exchange symbol
exchange = st.text_input("Enter the exchange symbol (e.g., BTC-USD):", "BTC-USD")

if st.button("Load Data"):
    # Make a POST request to the FastAPI endpoint
    response = requests.post(FASTAPI_URL, json={"exchange": exchange})
    
    # Check the response
    if response.status_code == 200:
        st.success(response.json().get("message"))
    else:
        st.error(f"Failed to load data: {response.json().get('detail')}")

