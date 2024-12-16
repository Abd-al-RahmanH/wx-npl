import requests
import json
import streamlit as st
from pyhive import presto
import ssl
import boto3

# COS Configuration
COS_ENDPOINT = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
COS_ACCESS_KEY_ID = "5e44ab1e0f314fc0846b3f3927b4d425"
COS_SECRET_ACCESS_KEY = "3200f01cadfb8eeca4ad714684974237a845903b3f3e32a9"
COS_BUCKET_NAME = "rahmanscos1"
CERTIFICATE_OBJECT_NAME = "presto.crt"
LOCAL_CERTIFICATE_PATH = "./presto.crt"

# Presto Configuration
PRESTO_HOST = "useast.services.cloud.techzone.ibm.com"
PRESTO_PORT = 39774
PRESTO_CATALOG = "tpch"
PRESTO_SCHEMA = "sf100"
USERNAME = "ibmlhadmin"
PASSWORD = "password"

# Watsonx.ai API Configuration
WATSONX_AI_API_URL = "https://us-south.ml.cloud.ibm.com/ml/v1/text/generation?version=2023-05-29"  # Replace with actual endpoint
WATSONX_AI_API_KEY = "r6zSAPJm7t8GbkqJENPzmXPpOKokltDGcMREKRr5fWdh"  # Replace with your API key
WATSONX_AI_PROJECT_ID = "833c9053-ef07-455e-819f-6557dea2f8bc"       # Replace with your project ID


# Function to initialize the COS client
def initialize_cos_client():
    return boto3.client(
        "s3",
        endpoint_url=COS_ENDPOINT,
        aws_access_key_id=COS_ACCESS_KEY_ID,
        aws_secret_access_key=COS_SECRET_ACCESS_KEY,
    )

# Function to download the certificate from COS
def download_certificate():
    cos_client = initialize_cos_client()
    cos_client.download_file(
        Bucket=COS_BUCKET_NAME,
        Key=CERTIFICATE_OBJECT_NAME,
        Filename=LOCAL_CERTIFICATE_PATH,
    )

# Function to query Watsonx.ai for SQL generation
def query_watsonx_ai(natural_language_question, table_schema):
    payload = {
        "model_id": "meta-llama/llama-3-405b-instruct",
        "data": {
            "input": natural_language_question,
            "instruction": f"You are a developer writing SQL queries given natural language questions. The database contains a set of 4 tables. The schema of each table with a description of the attributes is given. Write the SQL query given a natural language statement.\n\n{table_schema}",
            "input_prefix": "Input:",
            "output_prefix": "Output:",
            "examples": []
        },
        "parameters": {
            "decoding_method": "greedy",
            "stop_sequences": ["Input:"],
            "min_new_tokens": 1,
            "max_new_tokens": 200
        }
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {WATSONX_AI_API_KEY}",
        "x-project-id": WATSONX_AI_PROJECT_ID
    }
    response = requests.post(WATSONX_AI_API_URL, headers=headers, json=payload)
    response.raise_for_status()
    return response.json().get("output", "SQL generation failed.")

# Function to execute SQL query on Presto
def query_presto(sql_query):
    try:
        conn = presto.connect(
            host=PRESTO_HOST,
            port=PRESTO_PORT,
            username=USERNAME,
            password=PASSWORD,
            catalog=PRESTO_CATALOG,
            schema=PRESTO_SCHEMA,
            protocol='https',
            requests_kwargs={"verify": LOCAL_CERTIFICATE_PATH}
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        return cursor.fetchall()
    except Exception as e:
        return f"Error querying Presto: {e}"

# Streamlit App
def main():
    st.title("Natural Language to SQL with Watsonx.ai and Presto")

    # User Input for Natural Language Question
    natural_language_question = st.text_input("Enter your question:", placeholder="E.g., What is the total revenue for each product?")
    
    # Schema Definition
    table_schema = """
    (1) Database Table Name: postgresql.public.product
    (2) Database Table Name: postgresql.public.customer
    (3) Database Table Name: cos.retail.Orders
    (4) Database Table Name: cos.retail.OrderDetails
    """

    if st.button("Generate SQL Query"):
        with st.spinner("Generating SQL query..."):
            sql_query = query_watsonx_ai(natural_language_question, table_schema)
            st.code(sql_query, language="sql")

            with st.spinner("Executing SQL query on Presto..."):
                query_results = query_presto(sql_query)
                if isinstance(query_results, str):  # Error occurred
                    st.error(query_results)
                else:
                    st.write("Query Results:")
                    st.dataframe(query_results)

if __name__ == "__main__":
    # Ensure certificate is downloaded
    download_certificate()
    main()
