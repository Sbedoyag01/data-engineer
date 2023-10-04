import xxlimited
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define la configuración del DAG
dag = DAG(
    'mi_dag',
    description='Ejecución diaria de mi script en Docker',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 3),
    catchup=False
)

# Define una tarea que ejecutará tu script
run_script_task = PythonOperator(
    task_id='ejecutar_script',
    python_callable=data_api_emp.main,
    dag=dag
)
