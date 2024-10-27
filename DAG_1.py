from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция, которая будет выполнена в PythonOperator
def print_execution_date(ds, **kwargs):
    print(f"Дата выполнения DAG: {ds}")
    print("Дополнительное сообщение: задача выполнена успешно!")

# Определение DAG
with DAG(
    dag_id='example_bash_python_dag',
    start_date=datetime(2023, 10, 1),  # Дата начала выполнения DAG
    schedule='@daily',                 # Измените на `schedule` вместо `schedule_interval`
    catchup=False                      # Отключаем догоняющие запуски
) as dag:

    # Задача BashOperator
    bash_task = BashOperator(
        task_id='print_working_directory',
        bash_command='pwd'
    )

    # Задача PythonOperator
    python_task = PythonOperator(
        task_id='print_execution_date',
        python_callable=print_execution_date,
        op_args=['{{ ds }}']  # Передаем шаблон ds в функцию
    )

    # Устанавливаем порядок выполнения задач
    bash_task >> python_task
