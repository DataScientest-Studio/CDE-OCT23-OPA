import streamlit as st
import graphviz


# Create a graphlib graph object
graph = graphviz.Digraph()
graph.edge("run", "intr")
graph.edge("intr", "runbl")
graph.edge("runbl", "run")
graph.edge("run", "kernel")
graph.edge("kernel", "zombie")
graph.edge("kernel", "sleep")
graph.edge("kernel", "runmem")
graph.edge("sleep", "swap")
graph.edge("swap", "runswap")
graph.edge("runswap", "new")
graph.edge("runswap", "runmem")
graph.edge("new", "runmem")
graph.edge("sleep", "runmem")

st.graphviz_chart(graph)

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