"""
Marketplace ETL DAG
Orchestrates the ETL pipeline for marketplace inventory data.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract import extract_data
from transform import transform_data
from load import load_data
from utils import setup_logging, get_execution_stats


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'marketplace_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Marketplace Inventory Data',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['marketplace', 'etl', 'postgres'],
)


def extract_task(**context):
    """
    Extract data from JSON/CSV files.
    """
    logger = setup_logging()
    logger.info("Starting EXTRACT phase...")
    
    start_time = datetime.now()
    
    # Extract from JSON (change to 'csv' if needed)
    df = extract_data(data_dir='/opt/airflow/data', source_type='json')
    
    end_time = datetime.now()
    
    # Push data to XCom for next task
    context['task_instance'].xcom_push(key='raw_data_shape', value=df.shape)
    context['task_instance'].xcom_push(key='extract_time', value=(end_time - start_time).total_seconds())
    
    logger.info(f"EXTRACT complete: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Save to temporary file for next task
    temp_file = '/opt/airflow/data/temp_raw_data.csv'
    df.to_csv(temp_file, index=False)
    context['task_instance'].xcom_push(key='temp_file', value=temp_file)
    
    return df.shape[0]


def transform_task(**context):
    """
    Transform and clean the data.
    """
    logger = setup_logging()
    logger.info("Starting TRANSFORM phase...")
    
    start_time = datetime.now()
    
    # Get temp file from previous task
    import pandas as pd
    temp_file = context['task_instance'].xcom_pull(task_ids='extract', key='temp_file')
    raw_df = pd.read_csv(temp_file)
    
    # Transform
    stores_df, products_df = transform_data(raw_df)
    
    end_time = datetime.now()
    
    # Save transformed data
    stores_file = '/opt/airflow/data/temp_stores.csv'
    products_file = '/opt/airflow/data/temp_products.csv'
    
    stores_df.to_csv(stores_file, index=False)
    products_df.to_csv(products_file, index=False)
    
    context['task_instance'].xcom_push(key='stores_file', value=stores_file)
    context['task_instance'].xcom_push(key='products_file', value=products_file)
    context['task_instance'].xcom_push(key='stores_count', value=len(stores_df))
    context['task_instance'].xcom_push(key='products_count', value=len(products_df))
    context['task_instance'].xcom_push(key='transform_time', value=(end_time - start_time).total_seconds())
    
    logger.info(f"TRANSFORM complete: {len(stores_df)} stores, {len(products_df)} products")
    
    return {'stores': len(stores_df), 'products': len(products_df)}


def load_stores_task(**context):
    """
    Load stores data into PostgreSQL.
    """
    logger = setup_logging()
    logger.info("Starting LOAD STORES phase...")
    
    import pandas as pd
    from load import load_stores, get_db_connection
    
    stores_file = context['task_instance'].xcom_pull(task_ids='transform', key='stores_file')
    stores_df = pd.read_csv(stores_file)
    
    engine = get_db_connection()
    stores_loaded = load_stores(stores_df, engine)
    
    logger.info(f"LOAD STORES complete: {stores_loaded} stores loaded")
    
    return stores_loaded


def load_products_task(**context):
    """
    Load products data into PostgreSQL.
    """
    logger = setup_logging()
    logger.info("Starting LOAD PRODUCTS phase...")
    
    import pandas as pd
    from load import load_products, get_db_connection
    
    products_file = context['task_instance'].xcom_pull(task_ids='transform', key='products_file')
    products_df = pd.read_csv(products_file)
    
    engine = get_db_connection()
    products_loaded = load_products(products_df, engine)
    
    logger.info(f"LOAD PRODUCTS complete: {products_loaded} products loaded")
    
    return products_loaded


def validate_task(**context):
    """
    Validate data integrity after loading.
    """
    logger = setup_logging()
    logger.info("Starting VALIDATION phase...")
    
    from load import validate_data, get_db_connection
    
    engine = get_db_connection()
    validation_results = validate_data(engine)
    
    # Check for orphaned products
    if validation_results['orphaned_products'] > 0:
        logger.warning(f"Found {validation_results['orphaned_products']} orphaned products!")
    else:
        logger.info("No orphaned products found - data integrity OK")
    
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    return validation_results


def cleanup_task(**context):
    """
    Cleanup temporary files.
    """
    logger = setup_logging()
    logger.info("Cleaning up temporary files...")
    
    import os
    
    temp_files = [
        context['task_instance'].xcom_pull(task_ids='extract', key='temp_file'),
        context['task_instance'].xcom_pull(task_ids='transform', key='stores_file'),
        context['task_instance'].xcom_pull(task_ids='transform', key='products_file'),
    ]
    
    for temp_file in temp_files:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)
            logger.info(f"Removed: {temp_file}")
    
    logger.info("Cleanup complete")


# Define tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

load_stores = PythonOperator(
    task_id='load_stores',
    python_callable=load_stores_task,
    provide_context=True,
    dag=dag,
)

load_products = PythonOperator(
    task_id='load_products',
    python_callable=load_products_task,
    provide_context=True,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate',
    python_callable=validate_task,
    provide_context=True,
    dag=dag,
)

cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_task,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract >> transform >> load_stores >> load_products >> validate >> cleanup
