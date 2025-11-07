import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"'
    )
    # 이전에 사용했던 dags_python_operator DAG 사용 #
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        logical_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_task