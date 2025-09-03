from airflow.decorators import dag, task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1),
     catchup=False,
     schedule=None,
     tags=["Arpita","dag3"])

def dag_gcs_to_bquery():

    # 1Check CSV file exists by using a sensor operator
    check_csv_file = GCSObjectExistenceSensor(
        task_id="check_csv_file",
        bucket="bkt-global-health-data",
        object="global_health_data.csv",
        poke_interval=30,
        timeout=300
    )

    # Load CSV to BigQuery - loading the csv file from Bucket to Bigquery staging dataset
    load_csv_delimiter = GCSToBigQueryOperator(
        task_id="tsk_gcs_to_bigquery",
        bucket="bkt-global-health-data",
        source_objects=["global_health_data.csv"],
        source_format="CSV",
        destination_project_dataset_table='primal-graph-468804-j8.staging_dataset.global_health_data',
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        field_delimiter=",",
        autodetect=True
    )

    # 3 Create country-specific tables
    # Task to create country-specific tables dynamically
    countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']
    country_table_tasks = []


    for country in countries:
            task = BigQueryInsertJobOperator(
                task_id=f"create_table_{country.lower()}",
                configuration={
                    "query": {
                        "query": f"""
                            CREATE OR REPLACE TABLE `primal-graph-468804-j8.transformation_dataset.{country.lower()}_table` AS
                            SELECT *
                            FROM `primal-graph-468804-j8.staging_dataset.global_health_data`
                            WHERE country = '{country}'
                        """,
                        "useLegacySql": False,
                    }
                }
            )
            
            load_csv_delimiter >> task 
    # Set dependencies
    check_csv_file >> load_csv_delimiter
 
  


dag_gcs_to_bquery()

