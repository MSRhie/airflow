from airflow.sdk import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * * ", # 매월 말일 00시 10분에 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={
            "START_DATE": "{{ data_interval_start.in_timezone('Asia/Seoul') | ds }}", # 서울시간으로 바꾸고, yyy-dd-mm 포맷으로 변경(ds 필터 사용)
            "END_DATE": "{{ (data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}" # 서울시간으로 바꾸고, 1일 빼고, yyy-dd-mm 포맷으로 변경(ds 필터 사용)
        },
        bash_command='echo "START_DATE is $START_DATE , END_DATE is $END_DATE "'
    )