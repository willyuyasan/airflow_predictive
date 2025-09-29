import pandas as pd
import numpy as np
import logging
import datetime
from datetime import timedelta

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from integrations.postgres_connection import postgresConnection
from helpers.additional_functionalities import try_execution
from .queries import sp500_15_query



class getSp50015mHist:
    
    def __init__(self, **kwargs):
        
        self.conn_name = 'predictive'
        self.query_params = kwargs.get('query_params', None)
        self.query = self.prepares_query(sp500_15_query, self.query_params)
        self.df_output_name = 'stock_df'

        self.conns_str = kwargs.get('conns_str', None)
        self.class_name = type(self).__name__
    
    @try_execution
    def run(self):
    
        self.output_df = self.getdata(self.query, self.conn_name, self.conns_str)
        self.__rename_output()

    def prepares_query(self, query, query_parms):

        return query

    @try_execution
    def getdata(self, query, conn_name, conns_str):

        output_df = dummy_charged_df()

        query_success = False
        query_retries = 0

        while query_success==False:

            try:
                output_df = self.getdata2(query, conn_name, conns_str)
                query_success = True

                cols_ls = output_df.columns.tolist()
                logger.info('WU -> Columns found:')
                logger.info(cols_ls)

                df_shape = output_df.shape
                if df_shape[0]==0:
                    output_df = dummy_charged_df(cols_ls)

            except Exception as e:
                if "SSL error:" not in str(e):
                    logger.error(f'WU -> NO PRE-CHARGED DATA FOR: {self.class_name}')
                    raise
            
            query_retries = query_retries + 1

        return output_df

    @try_execution  
    def getdata2(self, query, conn_name, conns_str):
        
        get_connection = postgresConnection(conn_name, conns_str = conns_str)
        get_connection.run()
        conn = get_connection.connection
        engine = get_connection.engine
        engine.dispose()

        try:
            output_df = pd.read_sql(query, con=conn)
            conn.close()
            v = output_df.shape
            logger.info(f'WU -> OBSERVED INFO FOR ({self.class_name}): {v} #####')
        except Exception as e:
            conn.close()
            #engine.dispose()
            raise

        return output_df
    
    def __rename_output(self):

        script = f'self.{self.df_output_name} = self.output_df'
        exec(script)
 
    

# Additional functionalities

def dummy_charged_df(precols_ls=None):

    if not precols_ls:

        precols_ls = ['index', 'Datetime', 'Close', 'High', 'Low', 'Open', 'Volume', 'Date', 'Time', 'Open_adj', 'created_at']
        
    dummy_ls = np.full(len(precols_ls), None).tolist()
    dummy_charged_df = pd.DataFrame([dummy_ls], columns=precols_ls).copy()

    return dummy_charged_df
    