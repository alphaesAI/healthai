"""
Structure Pipeline DAG - Postgres Extractor -> Transformer -> Elasticsearch
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

def run_postgres_extractor(**context):
    """Run postgres extractor."""
    manager = ExtractorManager()
    manager.run_extraction('postgres')
    return 'postgres_extraction_complete'

def run_postgres_transformer(**context):
    """Run postgres transformer."""
    runner = TransformerRunner()
    runner.run_transformer('postgres_transformer')
    return 'postgres_transformation_complete'

# Define the DAG
with DAG(
    'structure_pipeline',
    default_args=default_args,
    description='Structure Data Pipeline: Postgres Extractor -> Transformer',
    schedule_interval=None,  # Manual trigger or external scheduler
    catchup=False,
    tags=['structure', 'pipeline'],
) as dag:
    
    # Create task group for structure pipeline
    structure_group = TaskGroup("structure_tasks", dag=dag)
    
    # Define tasks
    postgres_extract_task = PythonOperator(
        task_id='postgres_extractor',
        python_callable=run_postgres_extractor,
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
        task_group=structure_group,
    )
    
    postgres_transform_task = PythonOperator(
        task_id='postgres_transformer',
        python_callable=run_postgres_transformer,
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
        task_group=structure_group,
    )
    
    # Set task dependencies: extractor -> transformer
    postgres_extract_task >> postgres_transform_task
