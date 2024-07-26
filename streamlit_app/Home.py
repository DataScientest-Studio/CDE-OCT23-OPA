import streamlit as st


# ---- PAGE SETUP ------
project_explanation = st.Page(
    page = "pages/project_explanation.py",
    title = "Project",
    icon= ":material/account_circle:",
    default = True
)

about_us = st.Page(
    page = "pages/about_us.py",
    title = "About us",
    icon = ":material/account_circle:"
    )

daily_analysis = st.Page(
    page = 'pages/daily_analysis.py',
    title = "Daily Analysis",
    icon = ":material/bar_chart:"
)

hourly_analysis = st.Page(
    page = "pages/hourly_analysis.py",
    title = "Hourly Analysis",
    icon = ":material/bar_chart:"
    )


kpi = st.Page(
    page = "pages/kpi.py",
    title = "KPI",
    icon = ":material/bar_chart:"
    )

streaming = st.Page(
    page = "pages/streaming.py",
    title = "Streaming",
    icon = ":material/bar_chart:"
    )

# ---- NAVIGATION SETUP
pg = st.navigation(
    {
        "Info": [project_explanation, about_us],
        "Analysis": [daily_analysis, hourly_analysis, kpi],
        "Streaming": [streaming]
            
    }
)

pg.run()