                                            
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["csv","bigquery"])

def dag_gcs_to_bquery():

  #task to load csv from GCS to Bigquery  

    load_csv_delimiter = GCSToBigQueryOperator(
    task_id="tsk_gcs_to_bigquery",
    bucket="bkt-global-health-data",
    source_objects=["global_health_data/global_health_data.csv"],
    source_format="CSV",
    destination_project_dataset_table='primal-graph-468804-j8.staging_dataset.global_health_data',
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    field_delimiter=",",
    autodetect=True

        )


dag_gcs_to_bquery()    
