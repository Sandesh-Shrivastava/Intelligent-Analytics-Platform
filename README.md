# 🧠 Intelligent Analytics Platform

This repository contains the complete code for the Intelligent Analytics Platform. It is an end-to-end data product for an e-commerce business containing data ingestion, dbt transformations, machine learning models (XGBoost), Large Language Model features (RAG and Text-to-SQL via Groq Llama 3), and a Streamlit dashboard.

## Architecture
* **Cloud Infrastructure:** AWS S3 and AWS Athena
* **Data Transformation:** dbt (Data Build Tool)
* **Machine Learning:** XGBoost, Scikit-Learn, MLflow, SHAP
* **Generative AI:** Llama 3 70B (Text-to-SQL and RAG using ChromaDB)
* **Frontend:** Streamlit & Plotly

### Setup Instructons
1. Install requirements using `pip install -r requirements.txt`
2. Run the dashboard using `streamlit run dashboard/app.py`
