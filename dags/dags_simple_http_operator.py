import pendulum
from airflow.providers.http.operators.http import HttpOperator
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task


with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울 지하철 실시간 도착정보'''
    # SimpleHttpOperator -> HttpOperator로 변경되었습니다.
    # HttpOperator로 작성해주세요.
    tb_subway_station_info = HttpOperator(
        task_id='tb_subway_station_info',
        http_conn_id='swopenapi.seoul.go.kr/api/subway',
        endpoint='{{var.value.apikey_swopenapi_go_kr}}/json/realtimeStationArrival/0/5/서울/',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_subway_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_subway_station_info >> python_2()