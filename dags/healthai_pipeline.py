from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pipeline.connectors.factory import ConnectorFactory
from pipeline.extractors.factory import ExtractorFactory
from pipeline.transformers.factory import TransformerFactory


def run_connectors(**context):
    connectors = ConnectorFactory.load_from_yaml()
    results = {}

    for name, connector in connectors.items():
        results[name] = connector.fetch()

    context["ti"].xcom_push(key="connector_output", value=results)


def run_extractors(**context):
    connector_output = context["ti"].xcom_pull(
        key="connector_output", task_ids="connectors"
    )

    extractors = ExtractorFactory.load_from_yaml()
    results = {}

    for name, extractor in extractors.items():
        results[name] = extractor.extract(connector_output)

    context["ti"].xcom_push(key="extractor_output", value=results)


def run_transformers(**context):
    extractor_output = context["ti"].xcom_pull(
        key="extractor_output", task_ids="extractors"
    )

    transformers = TransformerFactory.load_from_yaml()
    results = {}

    for name, transformer in transformers.items():
        results[name] = transformer.transform(extractor_output)

    # final output → index / store / send to LLM
    print("Final transformed data:", results)


with DAG(
    dag_id="healthai_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["healthai"],
) as dag:

    connectors_task = PythonOperator(
        task_id="connectors",
        python_callable=run_connectors,
        provide_context=True,
    )

    extractors_task = PythonOperator(
        task_id="extractors",
        python_callable=run_extractors,
        provide_context=True,
    )

    transformers_task = PythonOperator(
        task_id="transformers",
        python_callable=run_transformers,
        provide_context=True,
    )

    connectors_task >> extractors_task >> transformers_task
