from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def test_submodule():
    """Testa se o submodule está funcionando"""
    print("🚀 Hello from DAGs submodule!")
    print("✅ Railway está reconhecendo o repositório de DAGs!")
    print(f"📅 Timestamp: {datetime.now()}")
    return "Submodule working!"

default_args = {
    'owner': 'railway-test',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_railway_submodule',
    default_args=default_args,
    description='Testa integração com Railway',
    schedule='@once',  # Executar apenas uma vez
    catchup=False,
    tags=['test', 'railway', 'submodule'],
) as dag:
    
    # Task 1: Verificar ambiente
    check_env = BashOperator(
        task_id='check_environment',
        bash_command='echo "Running on Railway! Host: $(hostname)"',
    )
    
    # Task 2: Testar Python
    test_python = PythonOperator(
        task_id='test_submodule_integration',
        python_callable=test_submodule,
    )
    
    # Task 3: Listar DAGs
    list_dags = BashOperator(
        task_id='list_available_dags',
        bash_command='ls -la $AIRFLOW_HOME/dags/',
    )
    
    check_env >> test_python >> list_dags
