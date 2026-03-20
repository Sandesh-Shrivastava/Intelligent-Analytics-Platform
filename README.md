# 🧠 Intelligent Analytics Platform

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-189fdd?style=for-the-badge)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)

🔴 **Live Application:** [https://intelligent-analytics-platform-rtaxz3a8dtnyafd32b9wco.streamlit.app](https://intelligent-analytics-platform-rtaxz3a8dtnyafd32b9wco.streamlit.app)

An end-to-end, FAANG-ready data product built for an e-commerce business. This platform bridges the gap between raw data and actionable business intelligence by combining Data Engineering, Data Analytics, Machine Learning, and Generative AI into a single, cohesive web application.

---

## 🚀 Project Overview

The Intelligent Analytics Platform takes raw e-commerce data (using the Brazilian Olist dataset) and transforms it into a live, interactive dashboard. It allows business stakeholders to view live KPIs, understand machine learning predictions for customer behavior, and even interact with their data using plain English via an integrated LLM.

### ✨ Key Features
1. **Live Business Dashboard:** An interactive, dark-themed Streamlit UI displaying revenue trends, top categories, and geographical sales mapping.
2. **Machine Learning Insights:**
   * **Churn Prediction:** An XGBoost classification model predicting which customers are at risk of leaving, explained via SHAP values.
   * **Customer Lifetime Value (CLV):** An XGBoost regression model predicting the future monetary value of customer segments.
   * **Product Recommender:** A collaborative filtering system suggesting untried product categories to existing customers.
3. **Generative AI Assistant ("Ask AI"):**
   * **Text-to-SQL:** Ask hard data questions (e.g., *"How many orders last month?"*) and the Llama 3 LLM will generate and execute an AWS Athena SQL query on the fly.
   * **RAG (Retrieval-Augmented Generation):** Ask business context questions (e.g., *"Why are we losing customers?"*) and the AI will search a local ChromaDB vector database to provide summarized insights.

---

## 🏗️ Architecture & Data Workflow

This project follows a modern data stack architecture:

1. **Ingestion (`/ingestion`):** Raw CSV data is uploaded to **AWS S3** and registered as external tables in **AWS Athena** using the `boto3` library.
2. **Transformation (`/dbt_project`):** **dbt (Data Build Tool)** is used to clean, model, and aggregate the raw Athena tables into optimized Data Marts (Fact and Dimension tables).
3. **Machine Learning (`/ml`):** Advanced feature engineering is performed on the dbt Marts to train XGBoost models. Experiments and metrics are tracked locally using **MLflow**.
4. **Artificial Intelligence (`/ai`):** A custom Python router directing user queries to either a Groq-powered LLM (for Text-to-SQL) or a HuggingFace/ChromaDB pipeline (for RAG).
5. **Presentation (`/dashboard`):** Everything is pulled together in an interactive **Streamlit** frontend optimized with `@st.cache_data` for blazing-fast Athena querying.

---

## 🛠️ Required Tech Stack

* **Cloud & Data Pipeline:** AWS S3, AWS Athena, dbt-athena-community, boto3, pyathena.
* **Machine Learning:** XGBoost, Scikit-Learn, SHAP, MLflow.
* **Generative AI:** Groq API (Llama 3.3 70B), ChromaDB (Vector DB).
* **Frontend:** Streamlit, Plotly Express.

---

## 💻 Setup & Installation Instructions

If you wish to run this project locally, follow these steps:

### 1. Requirements
Ensure you have Python 3.9+ installed. Clone this repository and install the dependencies:
```bash
pip install -r requirements.txt
```

### 2. Environment Variables
You must provide your own AWS and Groq API keys. Create a file named `.env` in the root directory (use the provided `.env.example` as a template) and add your keys:
```env
AWS_ACCESS_KEY_ID='your_aws_key'
AWS_SECRET_ACCESS_KEY='your_aws_secret'
AWS_REGION='eu-north-1'
S3_BUCKET_RAW='your_bucket_name'
ATHENA_DATABASE='raw_olist'
ATHENA_RESULTS_BUCKET='s3://your-athena-results-bucket'
GROQ_API_KEY='your_groq_key'
```

### 3. Run the Dashboard
Once your environment is set up and your AWS infrastructure contains the necessary data, you can launch the platform locally:
```bash
streamlit run dashboard/app.py
```
The dashboard will be available in your browser at `http://localhost:8501`.
