from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.extract import extract_clients, extract_orders, extract_products


def extraction_orders(**kwargs):
    print("Extraction des commandes...")
    date_obj = datetime.fromisoformat(kwargs["date"])
    print(date_obj)
    extract_orders(date_obj)
    

def extraction_customers(**kwargs):
    print("Extraction des clients...")
    date_obj = datetime.fromisoformat(kwargs["date"])
    print(date_obj)
    extract_clients(date_obj)

def extraction_products(**kwargs):
    print("Extraction des produits...")
    date_obj = datetime.fromisoformat(kwargs["date"])
    print(date_obj)
    extract_products(date_obj)
                     


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_ecommerce_extractions",
    default_args=default_args,
    description="DAG journalier pour extraire orders, customers, products",
    schedule="@daily",  # exécution quotidienne
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=["ecommerce", "extract"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_orders",
        python_callable=extraction_orders,
        op_kwargs={"date": "{{ data_interval_start }}"},
    )

    t2 = PythonOperator(
        task_id="extract_customers",
        python_callable=extraction_customers,
        op_kwargs={"date": "{{ data_interval_start }}"},
    )

    t3 = PythonOperator(
        task_id="extract_products",
        python_callable=extraction_products,
        op_kwargs={"date": "{{ data_interval_start }}"},
    )

    [t1, t2, t3]