import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, task_group, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="dags_branch_python_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']

    def common_func(**kwargs):
        print(kwargs['selected'])

    with TaskGroup(group_id='first_group', tooltip='첫 번째 그룹입니다.') as group_1:
        task_a = PythonOperator(
            task_id='task_a',
            python_callable=common_func,
            op_kwargs={'selected': 'A'}
        )

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다.') as group_2:
        task_b = PythonOperator(
            task_id='task_b',
            python_callable=common_func,
            op_kwargs={'selected': 'B'}
        )

        task_c = PythonOperator(
            task_id='task_c',
            python_callable=common_func,
            op_kwargs={'selected': 'C'}
        )

    select_random() >> Label('분기지점') >> [task_a, task_b, task_c]