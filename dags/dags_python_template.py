import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    # 1. jinja 템플릿 변수를 직접 넣어서 변환되는 과정을 보는 방법
    python_t1 = PythonOperator(
            task_id='python_t1',
            python_callable=python_function1,
            op_kwargs={'start_date': '{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}' }
    )

    # 2. kwargs 에 우리가 사용할 수 있는 템플릿 변수들이 있으니까 꺼내 쓰자
    @task(task_id='python_t2')
    def python_function2(**kwargs):
            print(kwargs)
            print('ds:' + kwargs['ds'])
            print('ts:' + str(kwargs['ts']))
            print('data_interval_start:' + str(kwargs['data_interval_start']))
            print('data_interval_end:' + str(kwargs['data_interval_end']))
            print('task_instance:' + str(kwargs['ti']))

    python_t1 >> python_function2()