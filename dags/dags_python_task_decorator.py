import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # [START howto_operator_python]
    @task(task_id="python_task_1") # python operator와 같이 task 이름은 자유롭게 지정가능
    def print_context(some_input):
        print(some_input)
    
    python_task_1 = print_context("task_decorator 실행") # 단, 여기서 실행할 task id는 같아야 함
    # [END howto_operator_python]