from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция для PythonOperator
def print_task_number(task_number):
    print(f"task number is: {task_number}")

# Объявление DAG
default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG('example_dag_with_30_tasks', default_args=default_args, schedule_interval='@daily') as dag:

    # Создаем 10 задач типа BashOperator
    for i in range(10):
        bash_task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f"echo {i}"
        )

    # Создаем 20 задач типа PythonOperator
    for i in range(10, 30):
        python_task = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
        

