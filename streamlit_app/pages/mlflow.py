import streamlit as st

# Title of the Streamlit app
st.title("Streamlit App with MLflow UI")

# Embed the MLflow UI using an iframe
st.markdown(
    f"""
    <iframe src="http://localhost:5000" width="100%" height="800px"></iframe>
    """,
    unsafe_allow_html=True
)

# Add other Streamlit components here if needed