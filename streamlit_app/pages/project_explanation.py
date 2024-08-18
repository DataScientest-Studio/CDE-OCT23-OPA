import streamlit as st
from PIL import Image

st.title("CryptoAnalytics")
st.header("Predict Tomorrow's Market Today")
st.write("""
         CryptoAnalytics is a cutting-edge API designed to deliver in-depth analysis and accurate predictions for the cryptocurrency market.
         """)
st.write("\n")
st.subheader("The three pillars of the project are the next:")
st.write("\n")


col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("<h2 style='text-align: left;'>DATA</h2>", unsafe_allow_html=True)
    icon_data = Image.open("./streamlit_app/files/source.png")
    st.image(icon_data, width=175)

with col2:
    st.markdown("<h2 style='text-align: center;'>ML</h2>", unsafe_allow_html=True)
    icon_ml = Image.open("./streamlit_app/files/ml.png")
    st.image(icon_ml, width=175)

with col3:
    st.markdown("<h2 style='text-align: center;'>Streaming</h2>", unsafe_allow_html=True)
    icon_streaming = Image.open("./streamlit_app/files/streaming.png")
    st.image(icon_streaming, width=175)
    
    
# --- DATA ---
st.write("\n")

st.subheader("Data source", anchor=False)
st.write(
    """
    - Data is extracted from: yfinance and Binance API
    - Is stored on MySQL and Cassandra databases
    - This data is used to do some analysis and dashboards
    """
)

st.write("Different dataframes availables:")


st.page_link("pages/daily_analysis.py", label="Daily data", icon=":material/today:")
st.page_link("pages/hourly_analysis.py", label="Hourly data", icon=":material/schedule:")
st.page_link("pages/kpi.py", label="KPI", icon=":material/candlestick_chart:")

    
# --- ML ---
st.write("\n")
st.subheader("Machine Learning", anchor=False)
st.write(
    """
    The steps of the machine learning has been:
    - Data modeling to convert _time series data into features_
    - Model is trained and registered using _mlflow_
    - The _mlflow UI's_ is an important tool to improve the models
    - The four models that are available are:
        - Linear Regression
        - XGBoost 
        - DecisionTreeRegressor
        - RandomForestRegressor
    - It's possible to check the graphs of:
        - Comparaison of the train and test 
        - The next 30 days prediction
    """
)

st.page_link("pages/ml_data_modeling.py", label="Data modeling", icon=":material/schema:")
st.page_link("pages/ml_models.py", label="ML Models", icon=":material/schema:")
st.page_link("pages/ml_models_metrics.py", label="ML Models Metrics", icon=":material/analytics:")
st.page_link("pages/predictions.py", label="Predictions", icon=":material/waterfall_chart:")


# --- Streaming ---
st.write("\n")
st.subheader("Streaming", anchor=False)
st.write(
    """
    - Example text
    """
)