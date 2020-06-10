import os
import fnmatch
import re

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from subdag_factory import subdag_factory
from airflow.models import Variable
from airflow.operators import CustomFileSensor

from datacleaner import data_cleaner
#from custom_file_sensor import CustomFileSensor

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
PARENT_DAG_NAME = 'store_dag'

filepath = Variable.get("sourcePath")
filepattern = Variable.get("filePattern")

default_args = {
    'owner': 'Airflow',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def process_file(**context):
    file_to_process = context['task_instance'].xcom_pull(key='file_name', task_ids='sense_incoming_files')
    print(file_to_process)
    taskName = re.sub('.csv','',file_to_process)
    print(taskName)
    return taskName

def create_subdag_operator(dag):
    directory = os.listdir(filepath)
    
    sub_dag_ops = []
    for fileName in directory:
        if(fileName.endswith(".csv")):
            subdag_name = re.sub('.csv','',fileName)
            sub_dag_op = SubDagOperator(
                            subdag = subdag_factory(PARENT_DAG_NAME,subdag_name,dag.start_date,dag.schedule_interval),
                            task_id = subdag_name
                            )
            sub_dag_ops.append(sub_dag_op)

    sense_files_task >> branch_task >> sub_dag_ops
    return sub_dag_ops



with DAG(dag_id=PARENT_DAG_NAME,
        default_args=default_args,
        schedule_interval='@daily', 
        start_date=datetime(2020,1,1),
        catchup=False
        ) as dag:

                sense_files_task = CustomFileSensor(task_id='sense_incoming_files',filepath=filepath,filepattern=filepattern,poke_interval=5,dag=dag)

                branch_task = PythonOperator(
                        task_id='process_file', 
                        provide_context=True,
                        python_callable=process_file, 
                        dag=dag
                        )
                
                sub_dag_task = create_subdag_operator(dag)
                             
                
                

    
