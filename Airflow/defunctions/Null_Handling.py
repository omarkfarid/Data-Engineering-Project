import pandas as pd
import numpy as np
from airflow import DAG
from airflow.models.xcom import XCom


def read_from_csv():
    df = pd.read_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv') 
    return df

def multiple_null_remover (df, threshhold):
    null_count = df.isnull().sum(axis=1)
    targeted_rows = df[null_count >= threshhold]
    df.drop(targeted_rows.index ,inplace = True)



df_clean = read_from_csv()


# Data set nulls handling
for col in df_clean.columns:
    df_clean[col] = df_clean[col].replace('-1',np.nan).replace(-1.0,np.nan).replace('Unknown / Non-Applicable',np.nan)


df_clean = df_clean.dropna(how = 'all')

multiple_null_remover(df_clean,4)


## Salary Cleaning
df_clean['Avg_Salary'] = df_clean['Salary_Estimate'].apply(lambda x: (int(x.split('-')[0]) + int(x.split('-')[1]))/2)
df_clean['Minimum_Salary'] = df_clean['Salary_Estimate'].apply(lambda x: (int(x.split('-')[0])))
df_clean['Maximum_Salary'] = df_clean['Salary_Estimate'].apply(lambda x: (int(x.split('-')[1])))


## Rating Null Values
df_clean['Rating'].fillna(df_clean['Rating'].mean(), inplace= True)


## Size Null Values
df_clean['Size'].fillna(df_clean['Size'].mode()[0],inplace = True)

## Founded missing values
mapping = dict(df_clean.groupby('Size')['Founded'].mean().sort_values(ascending=False))
df_clean['Founded'].fillna(df_clean['Size'].map(mapping),inplace= True)
df_clean['Founded'] = df_clean['Founded'].astype('int')

## Revenue Null Values
sector_map = dict(df_clean.groupby('Sector')['Revenue_(USD)'].apply(lambda x: x.mode()[0] if not x.mode().empty else '100-500 million '))
df_clean['Revenue_(USD)'].fillna(df_clean['Sector'].map(sector_map),inplace=True)

df_clean.at[168, 'Sector'] = 'Manufacturing'
df_clean.at[168, 'Industry'] = 'Industrial Manufacturing'

df_clean.at[272, 'Sector'] = 'Telecommunications'
df_clean.at[272, 'Industry'] = 'Telecommunications Services'


df_clean.to_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv',index=False)

