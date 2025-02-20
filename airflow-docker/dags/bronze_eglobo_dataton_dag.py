from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# Importando sua classe
from modules.bronze_eglobo_dataton import BronzeEgloboDataton

# Definindo argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'bronze_eglobo_dataton_dag',
    default_args=default_args,
    description='Uma DAG para processar dados brutos e salvar na camada bronze',
    schedule_interval=timedelta(days=1),  # Executa diariamente
    catchup=False,
)

# Função que será executada pela tarefa
def process_data():
    bronzeEgloboDataton = BronzeEgloboDataton()
    bronzeEgloboDataton.main()

# Definindo a tarefa
process_task = PythonOperator(
    task_id='process_raw_data',
    python_callable=process_data,
    dag=dag,
)

# Definindo a ordem das tarefas (neste caso, apenas uma tarefa)
process_task