from datetime import *
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.models.xcom import XCom


def read_from_csv():
    df = pd.read_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv') 
    return df

title_mapping = {'data scientist': 'Data Science',
                'data engineer':'Data Engineering',
                'machine learning':'AI / Machine Learning',
                'manager':'Management',
                'director': 'Management',
                'president': 'C-Level'}

level_mapping = {'sr':'Senior',
                 'senior':'Senior',
                'jr':'Junior',
                 'junior': 'Junior',
                'lead':'Lead',
                'Principal':'Principal'}

def job_type(title,title_mapping):
    for key, value in title_mapping.items():
        if key in title.lower():
            return value
        else:
            continue
    return 'Other'

def job_level(title,level_mapping):
    for key, value in level_mapping.items():
        if key in title.lower():
            return value
        else:
            continue
    return 'Associate'

df_clean = read_from_csv()

df_clean['Job_Type'] = df_clean['Job_Title'].apply(job_type,title_mapping = title_mapping)
df_clean['Job_Level'] = df_clean['Job_Title'].apply(job_level,level_mapping = level_mapping)

## Companies' Age
df_clean['Company\'s_Age'] = datetime.now().year - df_clean['Founded']
df_clean['Company\'s_Age'] = df_clean['Company\'s_Age'].astype('int')

## Age descritization
df_clean['Age_Category'] = pd.cut(df_clean['Company\'s_Age'], bins = [0,15,50,100,250], labels = ['New', 'Established', 'Experienced', 'Long-standing'], include_lowest= True)

## Salary binning
salary_strength = ['Very Low','Low Salary', 'Moderate', 'High']
salary_ranges = [20, 100,130, 180, 200]
df_clean['Salary_Strength'] = pd.cut(df_clean['Avg_Salary'], bins=salary_ranges, labels=salary_strength, right=False)


df_clean['Type_of_ownership_original'] = df_clean['Type_of_ownership']
df_clean = pd.get_dummies(df_clean, columns = ['Type_of_ownership'])

df_clean.to_csv('/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Cleaned_DS_Jobs.csv',index=False)

