from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import random
import string
from multiprocessing import Pool

# Параметры
FILE_COUNT = 100
FILE_LENGTH = 1000
OUTPUT_DIR = "/Users/baldens/airflow/dags/output_files" 

def create_files():
    print("START")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for i in range(FILE_COUNT):
        content = ''.join(random.choices(string.ascii_lowercase, k=FILE_LENGTH))
        with open(f"{OUTPUT_DIR}/file_{i}.txt", "w") as file:
            file.write(content)

def count_a(file_index):
    with open(f"{OUTPUT_DIR}/file_{file_index}.txt", "r") as file:
        text = file.read()
        count = text.count('a')
        with open(f"{OUTPUT_DIR}/{file_index}.res", "w") as res_file:
            res_file.write(str(count))

def sum_results():
    total_count = 0
    for i in range(FILE_COUNT):
        with open(f"{OUTPUT_DIR}/{i}.res", "r") as res_file:
            total_count += int(res_file.read().strip())
    print("Total 'a' count:", total_count)

with DAG(
    'count_a_dag',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1)
) as dag:

    create_files_task = PythonOperator(
        task_id='create_files',
        python_callable=create_files
    )

    parallel_tasks = []
    for i in range(FILE_COUNT):
        task = PythonOperator(
            task_id=f'count_a_in_file_{i}',
            python_callable=count_a,
            op_args=[i]
        )
        parallel_tasks.append(task)

    sum_results_task = PythonOperator(
        task_id='sum_results',
        python_callable=sum_results
    )

    create_files_task >> parallel_tasks >> sum_results_task