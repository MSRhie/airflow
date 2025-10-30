import pendulum
import datetime
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from plugins.common.common_func import get_sftp


with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    py_t1 = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )

    py_t1
