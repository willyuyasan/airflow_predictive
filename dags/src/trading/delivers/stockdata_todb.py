import pandas as pd
import numpy as np
import time
import datetime

from helpers.additional_functionalities import try_execution
from integrations.postgres_connection import postgresConnection
from delivers.db_delivery import dbDelivery

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


class stockDataToDb:
    
    def __init__(self, inputs_dict, **kwargs):
        
        self.inputs_dict = inputs_dict
        self.params_dict = kwargs.get('params_dict',None)
        self.prepare_params()
    
    @try_execution
    def run(self):
        
        outputs_dict = self.prepares_bd_inputs(self.inputs_dict, stock_desc=self.stock_desc)
        self.clear_db_tables(outputs_dict, self.conns_str, stock_desc=self.stock_desc)
        self.persist_data(outputs_dict, self.conns_str, stock_desc=self.stock_desc)

    def prepare_params(self):

        self.conns_str = self.inputs_dict['conns_str']

        try:
            self.stock_desc = self.params_dict['stock_desc']
        except:
            self.stock_desc = None

    def prepares_bd_inputs(self, inputs_dict, **kwargs):

        stock_df = inputs_dict['data_e2_df'].copy()
        new_stock_df = inputs_dict['data_e2_df'].copy()
        stock_desc = kwargs.get('stock_desc', None)

        if stock_desc in ['sp500_15m']:

            stock_df['Datetime'] = stock_df['Datetime'].apply(lambda x: str(x))
            stock_df['Datetime'] = stock_df['Datetime'].astype('str')

            new_stock_df['Datetime'] = new_stock_df['Datetime'].apply(lambda x: str(x))
            new_stock_df['Datetime'] = new_stock_df['Datetime'].astype('str')
        
            #Detects worked stocks
            generated_dt_ls = new_stock_df['Datetime'].tolist()
            generated_dt_ls = np.unique(np.array(generated_dt_ls)).tolist()
            v = new_stock_df.shape
            logger.info(f'WU -> Stocks Datetime currently worked: {v}')

            #Filters new stocks
            updated_dt_df = stock_df[stock_df['Datetime'].isin(generated_dt_ls)].copy()
            v = updated_dt_df.shape
            logger.info(f'WU -> Stocks to be updated: {v}')
        
        else:
            logger.error(f'WU -> Please, provide a stock description')
            raise

        outputs_dict = {
            'stock_df': updated_dt_df.copy(),
            'new_stock_df': new_stock_df.copy(),
        } 

        return outputs_dict
    
    def clear_db_tables(self, inputs_dict, conns_str, **kwargs):

        stock_df = inputs_dict['stock_df'].copy()
        stock_desc = kwargs.get('stock_desc', None)
        
        stock_dt_ls = stock_df['Datetime'].astype('str').tolist()
        stock_dt_ls = np.unique(np.array(stock_dt_ls)).tolist()
        v = len(stock_dt_ls)
        logger.info(f'WU -> Registers to be updated in Stocks: {v}')

        if stock_desc in ['sp500_15m']:

            #Clears (to be duplicated data)
            query = """
            delete from stocks.sp500_15m dd
            using (
            select
            unnest(array[{stock_dt_ls}])::timestamptz as "Datetime"
            ) d
            where 
            dd."Datetime" = d."Datetime"
            """

            query = query.format(stock_dt_ls=stock_dt_ls)

        try:
            executes_query(query, 'predictive', conns_str)
            v = len(stock_dt_ls)
            logger.info(f'WU -> Stocks deleted from ({stock_desc}): {v}')
        except Exception as e:
            logger.info(f'\nWU -> Data didnt clear by:\n{e}\n')
            pass
        
    def persist_data(self, inputs_dict, conns_str, **kwargs):

        new_stock_df = inputs_dict['new_stock_df'].copy()
        stock_desc = kwargs.get('stock_desc', None)

        delivery_dict = {}

        if stock_desc in ['sp500_15m']:

            delivery_dict['sp500_15m'] = new_stock_df.copy()

            dbdelivery = dbDelivery(
                conns_str = conns_str,
                conn_name = 'predictive',
                delivery_dict = delivery_dict,
                schema_name = 'stocks',
                option = 'append'
            )

            dbdelivery.run()



## ADITIONAL FUNCTIONALITIES:
def executes_query(input_query, conn_name, conns_str):

    get_connection = postgresConnection(conn_name, conns_str = conns_str)
    get_connection.run()
    conn = get_connection.connection

    try:
        conn.execution_options(autocommit=True).execute(input_query)
        logger.info(f'WU -> Query excecution success!')
    except Exception as e:
        try:
            logger.info(f'WU -> new pandas version detected!')
            cursor = conn.connection.cursor()
            cursor.execute(input_query)
            conn.connection.commit()
        except Exception as e:
            logger.error(f'WU -> Query excecution failed: {e}')
            raise

    logger.info(f'WU -> Query excecution success!')

    conn.close()
