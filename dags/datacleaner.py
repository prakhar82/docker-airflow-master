def data_cleaner():

    from datetime import datetime, timedelta
    import pandas as pd
    import os
    import re

    df = pd.read_csv("~/store_files_airflow/stores_transactions.csv")

    filePath = '/usr/local/airflow/store_files_airflow/clean_store_transactions.csv';
     # check clean_store_transactions exists then deleting them
    if os.path.exists(filePath):
        os.remove(filePath)
    filePath = '/usr/local/airflow/store_files_airflow/location_wise_profit.csv';
    # check clean_store_transactions exists then deleting them
    if os.path.exists(filePath):
        os.remove(filePath)
    filePath = '/usr/local/airflow/store_files_airflow/store_wise_profit.csv';
    # check clean_store_transactions exists then deleting them
    if os.path.exists(filePath):
        os.remove(filePath)

    yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%m-%d-%Y')

    def remove_currency_code(amount):
        return float(re.sub(r"[^0123456789\.]", '', amount))
    
    for to_clean in ['CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_currency_code(x))

    for to_change in ['TRANSACTION_DATE']:
        df[to_change] = df[to_change].map(lambda x: yesterday_date)
    
    df.to_csv('~/store_files_airflow/clean_store_transactions.csv', index=False)
