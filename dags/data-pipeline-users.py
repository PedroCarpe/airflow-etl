from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlite3
import os
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'users_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for users data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 22),
    catchup=False,
)

# Postgres setup function
def setup_postgres():
    conn = BaseHook.get_connection("my_postgres")

    host = conn.host
    user = conn.login
    password = conn.password
    database = conn.schema



def load_data_postgres(**context):
    print("Loading data to PostgreSQL...")

    # Get transformed data from previous task
    data = context['task_instance'].xcom_pull(task_ids='transform')

    # Connect to PostgreSQL using the Airflow connection
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres")


    # Insert data
    insert_sql = """
        INSERT INTO users (
            id,
            first_name,
            last_name,
            email,
            gender,
            ip_address
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            record['id'],
            record['first_name'],
            record['last_name'],
            record['email'],
            record['gender'],
            record['ip_address']
        )
        for record in data
    ]

    postgres_hook.insert_rows(
        table="users",
        rows=records,
        target_fields=[
            "id",
            "first_name",
            "last_name",
            "email",
            "gender",
            "ip_address"
        ]
    )

    print(f"Successfully loaded {len(records)} user records into PostgreSQL")    

# Database setup function
def setup_database():
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(BASE_DIR,'data','output','etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create users table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            gender TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Truncate the table
    cursor.execute('DELETE FROM users')
    conn.commit()
    conn.close()

# Define the extract function
def extract_data():
    print("Extracting data from users.csv...")

    BASE_DIR = os.path.dirname(os.path.dirname(__file__))

    csv_path = os.path.join(BASE_DIR, "data", "input", "users.csv")
    print(f"CSV PATH: {csv_path}")

    df = pd.read_csv(csv_path)
    return df.to_dict("records")

# Define the transform function
def transform_data(**context):
    print("Transforming data...")
    # Get data from previous task
    data = context['task_instance'].xcom_pull(task_ids='extract')
    
    # Add any transformations here if needed
    # For now, we'll just pass through the data
    return data

# Define the load function
def load_data(**context):
    print("Loading data to SQLite database...")
    # Get transformed data from previous task
    data = context['task_instance'].xcom_pull(task_ids='transform')
    
    # Connect to database
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(BASE_DIR,'data','output','etl_data.db')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Insert data
    for record in data:
        cursor.execute('''
            INSERT INTO users (id, first_name, last_name, email, gender, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            record['id'],
            record['first_name'],
            record['last_name'],
            record['email'],
            record['gender'],
            record['ip_address']
        ))
    
    conn.commit()
    conn.close()
    print(f"Successfully loaded {len(data)} user records into the database")

# Define the tasks
start_task = EmptyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data_postgres,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Add database setup task
setup_db_task = PythonOperator(
    task_id='setup_database',
    python_callable=setup_database,
    dag=dag,
)

# Add database setup task for Postgres
setup_postgres_task = PythonOperator(
    task_id='setup_postgres',
    python_callable=setup_postgres,
    dag=dag,
)

# Define the task dependencies
#start_task >> setup_postgres_task
#start_task >> setup_db_task >> extract_task >> transform_task >> load_task >> end_task
start_task >> extract_task >> transform_task >> load_task >> end_task
