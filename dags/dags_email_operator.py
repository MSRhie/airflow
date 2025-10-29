import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='conn_smtp_gmail',      # Airflow 3.0 실습부터 추가 # localhost:8080 에서 smtp 연결 설정 필요
        to='nanki1004@gmail.com',       # 본인의 메일 계정으로 변경해주세요.
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )