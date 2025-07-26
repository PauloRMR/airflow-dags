from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def print_hello():
    """Função simples que imprime uma mensagem"""
    print("Hello from the DAGs submodule!")
    print(f"Current time: {datetime.now()}")
    return "Task completed successfully"

def print_context(**context):
    """Função que mostra informações do contexto"""
    print(f"Execution date: {context['execution_date']}")
    print(f"DAG: {context['dag'].dag_id}")
    print(f"Task: {context['task'].task_id}")
    print("Context info printed successfully")

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir a DAG
with DAG(
    'hello_world_submodule',
    default_args=default_args,
    description='Teste de DAG no submodule',
    schedule='@hourly',  # Executar a cada hora
    catchup=False,
    tags=['teste', 'submodule'],
) as dag:
    
    # Task 1: Bash command
    t1 = BashOperator(
        task_id='print_date',
        bash_command='echo "DAG executada em: $(date)"',
    )
    
    # Task 2: Python function
    t2 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
    
    # Task 3: Python with context
    t3 = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        provide_context=True,
    )
    
    # Task 4: Final bash command
    t4 = BashOperator(
        task_id='final_task',
        bash_command='echo "Pipeline concluído com sucesso!"',
    )
    
    # Definir ordem de execução
    t1 >> t2 >> t3 >> t4
