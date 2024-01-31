from datetime import *
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.models.xcom import XCom


def read_from_csv():
    df = pd.read_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv') 
    return df



df_clean = read_from_csv()


def outlier_remover(df,column_name):
    median = np.median(df[column_name])
    upper_quartile = np.percentile(df[column_name], 75)
    lower_quartile = np.percentile(df[column_name], 25)

    iqr = upper_quartile - lower_quartile
    upper_whisker = df[column_name][df[column_name]<=upper_quartile+1.5*iqr].max()
    lower_whisker = df[column_name][df[column_name]>=lower_quartile-1.5*iqr].min()

    df = df[(df[column_name] < upper_whisker) & (df[column_name] > lower_whisker)]

    return df


df_clean = outlier_remover(df_clean,'Avg_Salary')
df_clean = outlier_remover(df_clean,'Company\'s_Age')




df_clean.to_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv',index=False)

