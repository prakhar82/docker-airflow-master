def data_cleaner():

    import pandas as pd
    import re

    df = pd.read_csv("~/store_files_airflow/stores_transactions.csv")

    def remove_currency_code(amount):
        return float(re.sub(r"[^0123456789\.]", '', amount))
    
    for to_clean in ['CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_currency_code(x))
    
    df.to_csv('~/store_files_airflow/clean_store_transactions.csv', index=False)
