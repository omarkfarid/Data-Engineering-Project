import pandas as pd
import numpy as np
from airflow import DAG
from airflow.models.xcom import XCom


def read_from_csv():
    df = pd.read_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Uncleaned_DS_jobs.csv') 
    return df

def columns_spaces (df):
    df.columns = df.columns.str.replace(' ','_')

def text_remover (df,column,chars):
    for c in chars:
        df[column] = df[column].apply(lambda x : str(x).replace(c,''))

def text_split (df, column, split_char, indx):
    df[column] = df[column].apply(lambda x : str(x).split(split_char)[indx])

def text_replace (df, column, char1, char2):
    df[column] = df[column].apply(lambda x : str(x).replace(char1,char2))

def multiple_null_remover (df, threshhold):
    null_count = df.isnull().sum(axis=1)
    targeted_rows = df[null_count >= threshhold]
    df.drop(targeted_rows.index ,inplace = True)


df_clean = read_from_csv()
print(df_clean)
## Drop Duplication
df_clean.drop_duplicates(keep = 'first', inplace = True)

## Remove Column name spaces
columns_spaces(df_clean)

## Salary Estimate Cleaning
text_split(df_clean,'Salary_Estimate', '(', 0)
text_remover(df_clean, 'Salary_Estimate', ['K','$'])


## Job Description Cleaning
text_remover(df_clean,'Job_Description',['\n','Job','Description','Summary','JOB DESCRIPTION:'])

# Cleaning company name from \n and rating
text_split(df_clean, 'Company_Name', '\n', 0)

# Company Size cleaning
text_remover(df_clean, 'Size', ['employees'])
text_replace(df_clean, 'Size', ' to ','-')
text_replace(df_clean, 'Size', 'Unknown','-1')



# Type of ownership cleaning
text_remover (df_clean,'Type_of_ownership',['Company - '])
text_split (df_clean, 'Type_of_ownership',' ',0)
text_replace (df_clean , 'Type_of_ownership', 'Unknown','-1')


# Revenue Cleaning
text_replace(df_clean, 'Revenue',' to ','-')
text_remover(df_clean,'Revenue',['$','(USD)'])

df_clean.rename(columns = {'Revenue':'Revenue_(USD)'},inplace= True)




df_clean.to_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv',index=False)

