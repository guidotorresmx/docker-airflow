from datetime import timedelta

from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from io import BytesIO

from datetime import datetime

import pyarrow.parquet as pq

####################################################################################

azure = WasbHook(wasb_conn_id='nds_ternium_azure_blob_storage')

default_args = {
    'owner': 'mijail',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['mreyes@ndscognitivelabs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'my_dag_test',
    default_args=default_args,
    description='My dag test desc.',
    schedule_interval="@once",
)

####################################################################################


def generate_date(**kwargs):
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S_")


def test_azure_blob(**kwargs):
    blob_prefix = azure.check_for_prefix(container_name='minutas', prefix='')
    print('blob_prefix: ', blob_prefix)

    blob_connection = azure.get_conn()
    print('blob_connection:', blob_connection)

    blob_list = blob_connection.list_blobs('minutas', prefix='')

    print('blob_list:', blob_list)
    parquet_name = ""
    for blob in blob_list:
        print("\t" + blob.name)
        parquet_name = blob.name

    print('parquet file: ', parquet_name)

    """ Get a dataframe from Parquet file on blob storage """
    byte_stream = BytesIO()
    try:
        blob_connection.get_blob_to_stream(
            container_name='minutas', blob_name=parquet_name, stream=byte_stream
        )
        df = pq.read_table(source=byte_stream).to_pandas()
    finally:
        byte_stream.close()

    print('dataframe:')
    print(df)

    """ Write Pandas dataframe to blob storage """
    buffer = BytesIO()
    df.to_parquet(buffer)
    blob_connection.create_blob_from_bytes(
        container_name='minutas', blob_name='new_file.parquet', blob=buffer.getvalue()
    )
    return True


task_generate_date = PythonOperator(
    task_id='generate_date',
    provide_context=True,
    python_callable=generate_date,
    dag=dag,
)


task_test_azure_blob = PythonOperator(
    task_id='test_azure_blob',
    python_callable=test_azure_blob,
    dag=dag)


task_generate_date >> task_test_azure_blob
