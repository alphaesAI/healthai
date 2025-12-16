from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import logging
import sys

# Add project root to Python path
project_root = str(Path(__file__).parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from pipeline.connectors.manager import ConnectorManager
from pipeline.extractors.manager import ExtractorManager

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'healthai',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': 0,
}

def run_extractor(extractor_name: str):
    """
    Run a single extractor as an Airflow task.
    
    Args:
        extractor_name: Name of the extractor to run (must match config)
    """
    try:
        logger.info(f"Starting {extractor_name} extraction")
        
        # Initialize managers
        connector_manager = ConnectorManager()
        extractor_manager = ExtractorManager()
        
        # Run extraction using manager
        extractor_manager.run_extraction(extractor_name)
        
        logger.info(f"Successfully completed {extractor_name} extraction")
        
    except Exception as e:
        logger.error(f"Error in {extractor_name} extraction: {str(e)}")
        raise

# Define the DAG
with DAG(
    'healthai_extraction_pipeline',
    default_args=default_args,
    description='HealthAI Data Extraction Pipeline',
    schedule_interval=None,  # Manual trigger or external scheduler
    catchup=False,
    tags=['healthai', 'extraction'],
) as dag:

    # Define tasks for each extractor
    postgres_task = PythonOperator(
        task_id='postgres_extractor',
        python_callable=run_extractor,
        op_kwargs={'extractor_name': 'postgres'},
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
    )

    gmail_task = PythonOperator(
        task_id='gmail_extractor',
        python_callable=run_extractor,
        op_kwargs={'extractor_name': 'gmail'},
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
    )

    elasticsearch_task = PythonOperator(
        task_id='elasticsearch_extractor',
        python_callable=run_extractor,
        op_kwargs={'extractor_name': 'elasticsearch'},
        retries=default_args['retries'],
        retry_delay=default_args['retry_delay'],
    )

    # Set task dependencies (run in parallel)
    [postgres_task, gmail_task, elasticsearch_task]

# DAG Documentation
dag.doc_md = """
# HealthAI Extraction Pipeline

## Overview
This DAG runs data extractors in parallel. Each extractor:
1. Reads its own configuration from extractor_config.yml
2. Creates and manages its own connectors
3. Extracts data from its internal resources (tables/labels/indices)
4. Saves output to JSON files

## Extractors
- postgres_extractor: Extracts data from PostgreSQL tables (users, orders, products)
- gmail_extractor: Extracts emails from Gmail labels (INBOX, SENT)
- elasticsearch_extractor: Extracts documents from Elasticsearch indices (user_events, audit_logs)

## Design Notes
- DAG orchestrates at extractor level only (no table/index/label tasks)
- Each extractor handles its own resource loops internally
- No XCom or in-memory data passing
- Output is written to filesystem
- Connector lifecycle is managed by extractors
"""