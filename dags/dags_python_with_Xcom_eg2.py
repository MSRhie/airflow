import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task

# return 문법을 사용한 Xcom Push와 Pull 예제
with DAG(
    dag_id="dags_python_with_Xcom_eg2",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    #Xcom Push 예제
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return '리턴 결과 : Success'


    #Xcom Pull 예제
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접찾은 리턴 값: ' + value1)
    
    @task(task_id='pythonxcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값: ' + status)

    # python_xcom_push_by_return 자체는 에어플로우 task 객체임. 단순히 "리턴 결과 : Success"만 가지고있지 않음
    python_xcom_push_by_return = xcom_push_result()
    # status 매개변수에 python_xcom_push_by_return의 리턴값을 넣어줌
    # xcom_push_result 함수 이후 xcom_pull_1, xcom_pull_2가 병렬로 실행됨
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()