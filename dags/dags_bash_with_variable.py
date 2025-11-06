import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models.variable import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # 첫번째 방법 #
    var_value = Variable.get("sample_key")
    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable: {var_value}"
    )

    # 두번째 방법 #
    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo variable: {{var.value.sample_key}}"
    )

    bash_var_1 >> bash_var_2