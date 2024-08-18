import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO

# API endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
METRICS = f"{FASTAPI_URL}/metrics"

def get_metrics():
    url = METRICS
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Error getting the metrics")
        return None

# Streamlit UI
st.title("ML: Model Metrics Dashboard")
st.subheader("View and analyze model metrics:")

data = get_metrics()

if data is None:
    st.write("No data available. Please load the data.")
else:
    try:
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
        
        desired_columns = ["Exchange", "Model", "Execution Date", "R2", "MAE", "MSE", "RMSE"]
        metrics_df = metrics_df[desired_columns]
        
        metrics_df = metrics_df.drop_duplicates()

    except Exception as e:
        st.error(f"An error occurred while processing the data: {e}")
        metrics_df = pd.DataFrame()  # Create an empty DataFrame to avoid errors below

with st.container():
    if not metrics_df.empty:
        col1, col2, col3, col4 = st.columns([1, 1, 2, 1])  # Adjust column width ratios

        with col1:
            exchanges = ["All"] + list(metrics_df['Exchange'].unique())
            exchange = st.selectbox("Exchange", sorted(exchanges))

        with col2:
            models = ["All"] + list(metrics_df['Model'].unique())
            model = st.selectbox("Model", sorted(models))
        
        with col3:
            sort_by = st.selectbox("Sort by", ["Execution Date", "R2", "MAE", "MSE", "RMSE"])
        
        with col4:
            sort_order = st.radio("Sort order", ["Ascending", "Descending"], index=0)

        # Filter dataframe based on selections
        filtered_df = metrics_df
        if exchange and exchange != "All":
            filtered_df = filtered_df[filtered_df['Exchange'] == exchange]
        if model and model != "All":
            filtered_df = filtered_df[filtered_df['Model'] == model]
        
        # Sort dataframe based on selected column and order
        if sort_by in filtered_df.columns:
            ascending = True if sort_order == "Ascending" else False
            filtered_df = filtered_df.sort_values(by=sort_by, ascending=ascending)
        
        st.dataframe(filtered_df)

        # Calculate R2 Mean
        r2_mean = filtered_df['R2'].mean()
        st.write(f"**R2 Mean**: {r2_mean:.2f}")

        # Plot Evolution Over Execution Date
        if 'Execution Date' in filtered_df.columns and not filtered_df.empty:
            st.subheader("Metrics Evolution Over Time")

            # Plot R2 Evolution
            plt.figure(figsize=(10, 6))
            sns.lineplot(data=filtered_df, x='Execution Date', y='R2', hue='Model', marker='o')
            plt.title('R2 Evolution Over Time')
            plt.xlabel('Execution Date')
            plt.ylabel('R2')
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save plot to BytesIO
            buf = BytesIO()
            plt.savefig(buf, format='png')
            st.image(buf, use_column_width=True)

            # Reset plot
            plt.close()

    else:
        st.write("No data to display.")
