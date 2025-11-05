import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task


with DAG(
    dag_id="dags_python_with_macro",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # python 오퍼레이터에서 날짜 계산
    @task(
        task_id='task_using_macros',
        templates_dict={ # 전월 1일 부터 전월 말일 까지
            'start_date': '{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds}}',
            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}'
        }   
    )
    def get_datetime_macro(**kwargs): # kwargs에 templates_dict가 딕셔너리 형태로 주입 됨
        templates_dict = kwargs.get('demplates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    # 파이썬 DAG내에서 날짜 직접 연산
    @task(
        task_id='task_direct_calc'
    )
    def get_datetime_calc(**kwargs):
        # 여기다가 적는 이유는 스케줄러 부하 감소를 위해서. as dag 아래나 위에 적으면 매번 문법검사 후 임포트 됨
        # 즉 오퍼레이터 안에서만사용할 라이브러리는 해당 오퍼레이터 안에 입력하는게 팁!
        from dateutil.relativedelta import relativedelta 
        data_interval_end = kwargs['data_interval_end']
    
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()