import streamlit as st

#st.logo()


# ---- PAGE SETUP ------

## DASHBOARD

daily_analysis = st.Page(
    page = 'pages/daily_analysis.py',
    title = "Daily Analysis",
    icon = ":material/today:"
)

hourly_analysis = st.Page(
    page = "pages/hourly_analysis.py",
    title = "Hourly Analysis",
    icon = ":material/schedule:"
    )


kpi = st.Page(
    page = "pages/kpi.py",
    title = "KPI",
    icon = ":material/candlestick_chart:"
    )

## STREAMING

streaming = st.Page(
    page = "pages/streaming.py",
    title = "Streaming",
    icon = ":material/timeline:"
    )

## MACHINE LEARNING

ml = st.Page(
    page = "pages/ml.py",
    title = "Data modeling",
    icon = ":material/schema:"
    )

ml_model = st.Page(
    page = "pages/ml_model.py",
    title = "ML Model",
    icon = ":material/query_stats:"
)

## PROJECT EXPLANATION

project_explanation = st.Page(
    page = "pages/project_explanation.py",
    title = "Project",
    icon= ":material/home:",
    default = True
)

about_us = st.Page(
    page = "pages/about_us.py",
    title = "About us",
    icon = ":material/account_circle:"
    )


# ---- NAVIGATION SETUP
pg = st.navigation(
    {
        "DASHBOARD ANALYSIS": [kpi, daily_analysis, hourly_analysis],
        "STREAMING": [streaming],
        "MACHINE LEARNING": [ml, ml_model],
        "PROJECT INFORMATION": [project_explanation, about_us]
            
    }
)

pg.run()