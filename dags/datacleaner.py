def data_cleaner():

    from datetime import datetime, timedelta
    import pandas as pd
    import re

    df = pd.read_csv("~/store_files_airflow/stores_transactions.csv")

    yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%m-%d-%Y')

    def remove_currency_code(amount):
        return float(re.sub(r"[^0123456789\.]", '', amount))
    
    for to_clean in ['CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_currency_code(x))

    for to_change in ['TRANSACTION_DATE']:
        df[to_change] = df[to_change].map(lambda x: yesterday_date)
    
    df.to_csv('~/store_files_airflow/clean_store_transactions.csv', index=False)
