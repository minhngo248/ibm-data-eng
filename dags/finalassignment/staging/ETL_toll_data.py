import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'ngocminhngo',
    'start_date': airflow.utils.dates.days_ago(0),
    'email': 'ngocminhngo@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

toll_dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

path_zip = '/home/project/airflow/dags/finalassignment/tolldata.tgz'
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {path_zip}',
    dag=toll_dag,
)

path_vehicle = '/home/project/airflow/dags/finalassignment/vehicle-data.csv'
saved_path = '/home/project/airflow/dags/finalassignment/csv_data.csv'
    
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'''
        cut -d ',' -f 1,2,3,4 {path_vehicle} > {saved_path}
    ''',
    dag=toll_dag,
)

path_tollplaza = '/home/project/airflow/dags/finalassignment/tollplaza-data.tsv'
saved_path_tollplaza = '/home/project/airflow/dags/finalassignment/tsv_data.csv'

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'''
        awk \'{{printf "%s,%s,%s\n", $1, $(NF-2), $NF}}\' {path_tollplaza} > {saved_path_tollplaza}
    ''',
    dag=toll_dag,
)

path_payment = '/home/project/airflow/dags/finalassignment/payment-data.txt'
saved_path_payment = '/home/project/airflow/dags/finalassignment/fixed_width_data.csv'

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'''awk \'{{printf "%s,%s\n", $(NF-1), $NF}}\' {path_payment} > {saved_path_payment}''',
    dag=toll_dag,
)


saved_path_consolidated = '/home/project/airflow/dags/finalassignment/extracted_data.csv'
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'''paste -d ',' {saved_path} {saved_path_tollplaza} {saved_path_payment} > {saved_path_consolidated} 
''',
    dag=toll_dag,
)

saved_path_final = '/home/project/airflow/dags/finalassignment/staging/transformed_data.csv'
# Transform vehicle type into capitalized string
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'''awk \'BEGIN {{FS = OFS = ","}} {{printf "%s,%s,%s,%s,%s,%s,%s,%s\n", 
            $1, $2, $3, $4, $5, $6, toupper($7), $8}}\' {saved_path_consolidated} > {saved_path_final}''',
    dag=toll_dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data