from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
from defunctions.read_csv import *



default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': None
}


dag = DAG(
    'DE_Project',
    default_args=default_args,
    description='Data Engineering Project'
)


Data_Reading = PythonOperator(
    task_id='Reading_CSV',
    python_callable = read,
    provide_context=True,
    dag=dag
)

basic_transform = PythonOperator(
    task_id='Basic_Cleaning',
    python_callable = lambda: exec(open("/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Transform.py").read()),
    provide_context=True,
    dag=dag
)


null_handling = PythonOperator(
    task_id='Null_Handling',
    python_callable = lambda: exec(open("/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/Null_Handling.py").read()),
    provide_context=True,
    dag=dag
)




feature_enineering = PythonOperator(
    task_id='feature_enineering',
    python_callable = lambda: exec(open("/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/feature_engineering.py").read()),
    provide_context=True,
    dag=dag
)



outliers = PythonOperator(
    task_id='outliers_removal',
    python_callable = lambda: exec(open("/home/lamloum/Python DE Project/venv/lib/python3.10/site-packages/defunctions/outliers.py").read()),
    provide_context=True,
    dag=dag
)
Data_Reading>>basic_transform>>null_handling>>feature_enineering>>outliers