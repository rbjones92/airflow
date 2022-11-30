from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2022,11,15)
}

dag = DAG(dag_id='market_vol', default_args=default_args, description = 'DL TSLA and APPL ticker data', schedule_interval='0 18 * * 1-5')

def get_data(ticker):
    import yfinance as yf
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)
    sdate = str(start_date)
    df = yf.download(ticker,start=start_date,end=end_date,interval='1m')
    filename = (f'/tmp/data/{ticker}_{sdate[0:10]}.csv')
    df = pd.DataFrame(df)
    df.to_csv(f'{filename}',header=True)

def query_data():
    import glob
    # read Apple data
    for f in glob.glob('/tmp/data/AAPL*'):
        df = pd.read_csv(f)
        df = df.describe()
        filename = (f'/tmp/data/AAPL_summary_stats.csv')
        df.to_csv(filename)
    for f in glob.glob('/tmp/data/TSLA*'):
        df = pd.read_csv(f)
        df = df.describe()
        filename = (f'/tmp/data/TSLA_summary_stats.csv')
        df.to_csv(filename)


make_data_directory = BashOperator(
    task_id = 'make_data_directory',
    bash_command = 'mkdir -p /tmp/data/',
    dag = dag
)

t1 = PythonOperator(
    task_id='pull_AAPL',
    # Add the callable
    python_callable=get_data,
    # Define the arguments
    op_kwargs={'ticker':'AAPL'},
    dag=dag
)

t2 = PythonOperator(
    task_id='pull_TSLA',
    # Add the callable
    python_callable=get_data,
    # Define the arguments
    op_kwargs={'ticker':'TSLA'},
    dag=dag
)


t3 = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    dag=dag
)

make_data_directory >> t1 >> t2 >> t3
