import os
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Corrected typo: 'depends_on_past' instead of 'deoends_on_past'
    'start_date': datetime(2023, 1, 1),  # Corrected date format (YYYY, MM, DD)
    'retries': 1,
}

with DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A simple DAG to load data from GCS to BigQuery',
    schedule_interval=None,
) as dag:

    proyect_id = os.environ.get("GCP_PROJECT")
    bucket_name = os.environ.get("GCS_BUCKET")

    # Tarea 1: Carga el archivo desde GCS a BigQuery
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='CREATE_DATASET',
        project_id=proyect_id,  # Added missing project_id
        dataset_id='my_dataset',
        location='us'
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id='CREATE_TABLE',
        project_id=proyect_id,  # Added missing project_id
        dataset_id='my_dataset',  # Corrected typo: 'my-dataset' to 'my_dataset'
        table_id='my_table',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'fecha', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'total_ventas', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=bucket_name,
        source_objects=['dags/file_upload/datos/ventas.csv'],  # Corrected path format
        destination_project_dataset_table=f'{proyect_id}.my_dataset.my_table',
        # destination_table=f'{proyect_id}.my_dataset.my_table',  # Corrected full table path
        write_disposition='WRITE_TRUNCATE',  # Corrected capitalization and underscore
        source_format='CSV',
    )

    # Tarea 2: Ejecuta la query de transformación
    sql = f'''
        # Escribe tu query SQL aquí
        SELECT
            AVG(total_ventas) as promedio_ventas, min(total_ventas) as minimo_ventas,
            max(total_ventas) as maximo_ventas, min(fecha) as inicio_periodo,
            max(fecha) as fin_periodo
        FROM
            {proyect_id}.my_dataset.my_table
    '''
    transform_data_task = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": proyect_id,
                    "datasetId": 'my_dataset',
                    "tableId": 'transformate_table',
                },
                "priority": "BATCH",
            }
        },
        # location="us-central1",  # Replace with your desired location
    )

    create_dataset >> create_table >> load_gcs_to_bq >> transform_data_task
