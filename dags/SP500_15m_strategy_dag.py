import sys
import datetime
from datetime import timedelta

import logging
# Configure the log
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from src.trading.SP500_15m_strategy import sp50015mStrategy

def sp50015mstrategy_f():

    try:
        logger.info(f'\n\nWU-> Process (sp500_15m_strategy) STARTED!\n\n')

        sp50015mstrategy = sp50015mStrategy()
        sp50015mstrategy.run()

        outputs_dict = sp50015mstrategy.outputs_dict
        sp50015mstrategy.send_notification(inputs_dict = outputs_dict)

        logger.info(f'\n\nWU-> Process (sp500_15m_strategy) FINISHED!\n\n')

    except Exception as e:
        logger.info(sys.exc_info())
        #TODO: Send email with the notification
        logger.error(f'\n\nWU-> Process (sp500_15m_strategy) FAILED!\n{e}\n')
        raise



START_DATE = datetime.datetime.now(datetime.timezone.utc) - timedelta(days=1)

default_args = {
    'owner': 'william_uyasan',
    "depends_on_past": False,
    "start_date": START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "max_active_runs": 1,
}

dag = DAG(dag_id = 'SP500_15m_strategy',
          catchup=False,
          default_args=default_args,
          schedule="*/15 13-20 * * MON-FRI",
          #schedule="*/15 * * * *",
          #schedule = None,
          start_date=START_DATE,
          max_active_runs=1)

dag_start = EmptyOperator(
    task_id='start',
    dag=dag,
)

sp50015mstrategy_op = PythonOperator(
    task_id='SP500_15m_strategy',
    python_callable=sp50015mstrategy_f,
    dag=dag,
)

dag_end = EmptyOperator(
    task_id='end',
    dag=dag,
)

dag_start >> sp50015mstrategy_op >> dag_end