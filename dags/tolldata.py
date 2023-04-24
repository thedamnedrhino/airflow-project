from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

default_args = {
        'owner': 'Farzad',
        'email_on_failure': True,
        'email_on_retry': True,
        'retry_delay': timedelta(minutes=5),
        'retries': 0,
        'start_date': datetime(year=2023, month=4, day=13, hour=15, minute=10)
        }


@task.virtualenv(task_id='read_csv', requirements=['pandas'])
def read_csv():
    import os
    import pandas as pd
    def path(file: str) -> str:
        return os.path.join('/opt/airflow/data', file)

    file_name = path('tolldata_unzipped/vehicle-data.csv')
    df = pd.read_csv(file_name, header=None, names=['Rowid', 'Timestamp', 'Anon Vehicle Num', 'Vehicle type', 'Number of axles', 'Vehicle code'])

    output_files = {
            'vehicle': path('tolldata_unzipped/csv.csv'),
        }
    df[['Rowid', 'Timestamp', 'Anon Vehicle Num', 'Vehicle type']].to_csv(output_files['vehicle'], index=False)


@task.virtualenv(task_id='read_tsv', requirements=['pandas'])
def read_tsv():
    import os
    import pandas as pd
    def path(file: str) -> str:
        return os.path.join('/opt/airflow/data', file)

    file_name = path('tolldata_unzipped/tollplaza-data.tsv')
    df = pd.read_csv(file_name, names=['Rowid', 'Timestamp', 'Anon Vehicle Number', 'Vehicle type', 'Num Axles', 'Tollplaza id', 'Tollplaza code'], sep='\t')

    output_files = {
            'tsv': path('tolldata_unzipped/tsv.csv'),
        }
    df[['Num Axles', 'Tollplaza id', 'Tollplaza code']].to_csv(output_files['tsv'], index=False)


@task.virtualenv(task_id='read_fwf', requirements=['pandas'])
def read_fwf():
    import os
    import pandas as pd
    def path(file: str) -> str:
        return os.path.join('/opt/airflow/data', file)

    file_name = path('tolldata_unzipped/payment-data.txt')

    df = pd.read_fwf(file_name, header=None,
                     names=['Rowid', 'Timestamp', 'Anon Vehicle Number', 'Tollplaza id', 'Tollplaza code', 'payment type', 'Vehicle code',],
                     widths=[6, 12 + 8 + 5, 8, 8, 10, 4, 6])

    output_files = {
            'fwf': path('tolldata_unzipped/fwf.csv'),
        }

    df[['payment type', 'Vehicle code']].to_csv(output_files['fwf'], index=False)


@task.virtualenv(task_id='merge', requirements=['pandas'])
def merge():
    import os
    import pandas as pd

    dfs = [pd.read_csv(os.path.join(path(file)))
           for file in ['tolldata_unzipped/' + x for x in ['csv.csv', 'tsv.csv', 'fwf.csv']]
           ]

    df = pd.concat(dfs, axis=1)

    df.to_csv(path('merged.csv'), index=False)


@task.virtualenv(task_id='transform', requirements=['pandas'])
def transform():
    import os
    import pandas as pd
    def path(file: str) -> str:
        return os.path.join('/opt/airflow/data', file)
    df = pd.read_csv(path('merged.csv'))
    df['Vehicle type'] = df['Vehicle type'].apply(lambda x: x.upper())
    df.to_csv(path('result.csv'), index=False)



with DAG(
        'a_main_dag',
        default_args=default_args,
        schedule=timedelta(days=1),
        catchup=False,
        ) as dag:
    download = BashOperator(task_id='download_file',
                            bash_command='cd $AIRFLOW_HOME/data; curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz --output tolldata.tgz;')
    unzip = BashOperator(task_id='unzip',
                              bash_command='cd $AIRFLOW_HOME/data; mkdir tolldata_unzipped; tar -xvzf tolldata.tgz -C tolldata_unzipped/;')
    empty_op = EmptyOperator(task_id='first_operator')
    cwd = BashOperator(task_id='cwd',
                       bash_command=f'echo "{os.getcwd()}" > cwd;')
    csv = read_csv()
    tsv = read_tsv()
    fwf = read_fwf()
    merge_task = merge()

    # chain(download, unzip, [csv, tsv, fwf], merge_task, transform)
    download >> unzip >> [csv >> tsv >> fwf] >> merge_task >> transform()

