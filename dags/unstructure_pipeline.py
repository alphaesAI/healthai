"""
Unstructure Pipeline DAG - Gmail Extractor -> Transformer
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from pipeline.extractors.manager import ExtractorManager
from pipeline.transformers.runner import TransformerRunner

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='UTC'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def run_gmail_extractor(**context):
    """Run gmail extractor."""
    manager = ExtractorManager()
    manager.run_extraction('gmail')
    return 'gmail_extraction_complete'

def run_gmail_transformer(**context):
    """Run gmail transformer."""
    runner = TransformerRunner()
    runner.run_transformer('gmail_transformer')
    return 'gmail_transformation_complete'

# Define the DAG
with DAG(
    'unstructure_pipeline',
    default_args=default_args,
    description='Unstructure Data Pipeline: Gmail Extractor -> Transformer',
    schedule_interval=None,  # Manual trigger or external scheduler
    catchup=False,
    tags=['unstructure', 'pipeline'],
) as dag:
    
    # Create task group for unstructure pipeline
    unstructure_group = TaskGroup("unstructure_tasks", dag=dag)
    
    # Define tasks
    gmail_extract_task = PythonOperator(
        task_id='gmail_extractor',
        python_callable=run_gmail_extractor,
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
        task_group=unstructure_group,
    )
    
    gmail_transform_task = PythonOperator(
        task_id='gmail_transformer',
        python_callable=run_gmail_transformer,
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
        task_group=unstructure_group,
    )
    
    # Set task dependencies: extractor -> transformer
    gmail_extract_task >> gmail_transform_task
