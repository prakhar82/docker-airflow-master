import requests,csv,json
import pandas as pd
from itertools import groupby 
from collections import OrderedDict
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from datacleaner import data_cleaner
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

filepath = Variable.get("sourcePath")
filepattern = Variable.get("filePattern")

def parse_csv_to_json_save(**kwargs):
    fileName = kwargs['child_dag_name']
    print(fileName)
    csvFilePath = filepath+fileName+'.csv'
    
    df = pd.read_csv(csvFilePath, 
                dtype={"STORE_ID" : str,"STORE_COUNTRY" : str,"STORE_LOCATION" : str,
                        "PRODUCT_CATEGORY" : str,"PRODUCT_ID" : str,
                        "MRP" : str,"CP" : str, "DISCOUNT" : str, "SP" : str, "TRANSACTION_DATE" : str})

    results = []

    for (product_category), bag in df.groupby(['PRODUCT_CATEGORY']):
        contents_df = bag.drop(['PRODUCT_CATEGORY'], axis=1)
        subset = [OrderedDict(row) for i,row in contents_df.iterrows()]
        results.append(OrderedDict([('PRODUCT_CATEGORY', product_category),
                                    ('product_details', subset)]))
    
    for i in range(len(results)):
        product = dict(results[i])
        print(product)
        postUrl = "http://host.docker.internal:8081/stores/upload"
        print(postUrl)
        r = requests.post(postUrl, json=product)
        k = r.status_code
 
def branch_func(**kwargs):
    timestamp = kwargs['child_dag_name']
    print("timestamp", timestamp)
    file_timestamp = timestamp[-8:]
    print("file_timestamp", file_timestamp)
    todaysDate = datetime.now()
    print("Today's date:", todaysDate)
    
    file_date = datetime.strptime(file_timestamp, '%d%m%Y')
    print("file_date",file_date)
    if file_date > todaysDate:
        return 'parse_csv_to_json_save'
    else:
        return 'stop_task'

def subdag_factory(parent_dag_name, child_dag_name, start_date, 
schedule_interval):
                    subdag = DAG(
                        dag_id = '{0}.{1}'.format(parent_dag_name,child_dag_name),
                        schedule_interval = schedule_interval,
                        start_date = start_date,
                        catchup = False)
                    with subdag:

                                #clean_data_task = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner,op_kwargs={'child_dag_name': child_dag_name})

                                #t3 = MySqlOperator(task_id='create_mysql_table', mysql_conn_id="mysql_connect", sql="create_table.sql")

                                #t4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id="mysql_connect", sql="insert_into_table.sql")

                                #t5 = MySqlOperator(task_id='select_from_table', mysql_conn_id="mysql_connect", sql="select_from_table.sql")
                                branch_op = BranchPythonOperator(task_id='branch_task',
                                            provide_context=True,
                                            python_callable=branch_func,
                                            op_kwargs={'child_dag_name': child_dag_name},
                                            dag=subdag)

                                save_data_task = PythonOperator(task_id='parse_csv_save_data', 
                                                python_callable=parse_csv_to_json_save,
                                                op_kwargs={'child_dag_name': child_dag_name},
                                                dag=subdag)
                                
                                stop_op = DummyOperator(task_id='stop_task',
                                                dag=subdag,
                                                trigger_rule='one_success')

                                branch_op >> save_data_task >> stop_op
                                branch_op >> stop_op


                    return subdag
