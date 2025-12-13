# /home/logi/github/alphaesai/healthai/dags/connector_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.connectors.manager import ConnectorManager

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'connector_pipeline',
    default_args=default_args,
    description='DAG for testing and managing connectors',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def execute_postgres_operations(**kwargs):
    """Execute operations using PostgreSQL connector."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'pipeline/connectors/connector_config.yml'
    )
    
    manager = ConnectorManager(config_path)
    
    try:
        # Get PostgreSQL connector
        postgres = manager.get_connector('postgres')
        postgres.connect()
        
        # Example 1: Get database version
        cursor = postgres._connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"PostgreSQL version: {version[0]}")
        
        # Example 2: Execute a query (modify as needed)
        # cursor.execute("SELECT * FROM your_table LIMIT 5;")
        # results = cursor.fetchall()
        # for row in results:
        #     print(row)
        
        cursor.close()
        postgres.disconnect()
        
    except Exception as e:
        print(f"Error in PostgreSQL operations: {e}")
        raise

postgres_task = PythonOperator(
    task_id='postgres_operations',
    python_callable=execute_postgres_operations,
    provide_context=True,
    dag=dag,
)

def execute_elasticsearch_operations(**kwargs):
    """Execute operations using Elasticsearch connector."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'pipeline/connectors/connector_config.yml'
    )
    
    manager = ConnectorManager(config_path)
    
    try:
        # Get Elasticsearch connector
        es = manager.get_connector('elasticsearch')
        es.connect()
        
        # Example 1: Get cluster info
        info = es._client.info()
        print(f"Cluster Name: {info.get('cluster_name')}")
        print(f"Elasticsearch Version: {info.get('version', {}).get('number')}")
        
        # Example 2: Search query (modify as needed)
        # es_query = {"query": {"match_all": {}}, "size": 5}
        # index_name = "your_index_name"
        # response = es._client.search(index=index_name, body=es_query)
        # print(f"Found {response['hits']['total']['value']} documents")
        
        es.disconnect()
        
    except Exception as e:
        print(f"Error in Elasticsearch operations: {e}")
        raise

elasticsearch_task = PythonOperator(
    task_id='elasticsearch_operations',
    python_callable=execute_elasticsearch_operations,
    provide_context=True,
    dag=dag,
)

def execute_gmail_operations(**kwargs):
    """Execute operations using Gmail connector."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'pipeline/connectors/connector_config.yml'
    )
    
    manager = ConnectorManager(config_path)
    
    try:
        # Get Gmail connector
        gmail = manager.get_connector('gmail')
        gmail.connect()
        
        # Example 1: Get Gmail profile
        profile = gmail.service.users().getProfile(userId='me').execute()
        print(f"Connected to Gmail account: {profile.get('emailAddress')}")
        
        # Example 2: List email labels
        print("\nAvailable email labels:")
        results = gmail.service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        for label in labels:
            print(f"- {label['name']} (ID: {label['id']})")
        
        # Example 3: Get recent emails (commented out as it requires more permissions)
        # results = gmail.service.users().messages().list(
        #     userId='me',
        #     labelIds=['INBOX'],
        #     maxResults=5
        # ).execute()
        # 
        # print("\nRecent emails:")
        # for msg in results.get('messages', [])[:5]:
        #     message = gmail.service.users().messages().get(
        #         userId='me',
        #         id=msg['id']
        #     ).execute()
        #     headers = {h['name']: h['value'] for h in message['payload']['headers']}
        #     print(f"{headers.get('From')} - {headers.get('Subject')}")
        
        gmail.disconnect()
        
    except Exception as e:
        print(f"Error in Gmail operations: {e}")
        raise

gmail_task = PythonOperator(
    task_id='gmail_operations',
    python_callable=execute_gmail_operations,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
# Run tasks in sequence: PostgreSQL -> Elasticsearch -> Gmail
postgres_task >> elasticsearch_task >> gmail_task