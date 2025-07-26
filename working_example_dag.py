from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Sintaxe correta - schedule ao invés de schedule_interval
with DAG(
    'working_example',
    default_args=default_args,
    description='DAG com sintaxe correta',
    schedule='@daily',  # CORRETO!
    catchup=False,
    tags=['example', 'working'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    task1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    
    task2 = BashOperator(
        task_id='print_message',
        bash_command='echo "DAG funcionando corretamente!"',
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> task1 >> task2 >> end
