import os
import time
import datetime as dt

from integrations.postgres_connection import postgresConnection
from helpers.additional_functionalities import try_execution

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


@try_execution
class dbDelivery:
    
    def __init__(self,  **kwargs):

        self.conns_str = kwargs.get('conns_str',None)
        self.delivery_dict = kwargs.get('delivery_dict',None)
        self.conn_name = kwargs.get('conn_name',None)
        self.schema_name = kwargs.get('schema_name','public')
        self.option = kwargs.get('option', 'append')

    @try_execution
    def run(self):

        self.delivery_dict = self.validates_incomming_columns(self.delivery_dict, self.conn_name, self.schema_name, self.conns_str)
        self.db_saving(self.delivery_dict, self.conn_name, self.schema_name, self.conns_str, option = self.option)          
    
    @try_execution
    def validates_incomming_columns(self, delivery_dict, conn_name, schema_name, conns_str):

        #Detecting new incomming columns to db storage
        def detecting_new_columns_todb(conns_str, conn_name, exporting_df, schema_name, db_table_name, **kwargs):
            
            option = kwargs.get('option',None)

            #gets the connection
            dbconnection = postgresConnection(conn_name, conns_str = conns_str)
            dbconnection.run()
            conn = dbconnection.connection

            mandatorycols_df = dbconnection.get_mandatory_columns(schema_name, db_table_name, conn)
            mandatorycols_ls = mandatorycols_df['column_name'].tolist()
            actualcols_ls = exporting_df.columns.tolist()

            pending_cols = [c for c in mandatorycols_ls if c not in actualcols_ls]
            if len(pending_cols)>0:
                logger.info(f'WU -> PENDING COLUMNS FOR {schema_name}.{db_table_name}: {len(pending_cols)}')
                logger.info(pending_cols)
            
            new_cols = [c for c in actualcols_ls if c not in mandatorycols_ls]
            if len(new_cols)>0:
                logger.info(f'WU -> NEW DETECTED COLUMNS COMING TO {schema_name}.{db_table_name}: {len(new_cols)}')
                logger.info(new_cols)

                if (option == 'match data') & (mandatorycols_ls!=[]):
                    match_cols = [c for c in actualcols_ls if c in mandatorycols_ls]
                    exporting_df = exporting_df[match_cols].copy()

            return exporting_df
        
        for k,df in delivery_dict.items():

            input_df = df.copy()
            input_df = detecting_new_columns_todb(conns_str, conn_name, input_df, schema_name, k, option = 'match data')
            delivery_dict[k] = input_df.copy()

        return delivery_dict

    @try_execution
    def db_saving(self, delivery_dict, conn_name, schema_name, conns_str, **kwargs):

        option = kwargs.get('option',None)

        def saves_df_intodb(conn_name, conns_str, input_df, dbtable_name, dbtable_schema, option):
            
            if input_df.empty==False:
                dbconnection = postgresConnection(conn_name, conns_str=conns_str)
                dbconnection.run()

                created_at = dt.datetime.now()
                input_df['created_at'] = created_at

                if len(input_df) > 0:

                    dbconnection.dftodb(
                        table_df=input_df, 
                        table_dbname=dbtable_name, 
                        schema=dbtable_schema, 
                        option=option
                        )
                    
                    v = input_df.shape
                    logger.info(f'WU-> DATA SAVED INTO: {dbtable_schema}.{dbtable_name} WITH {v} REGISTERS')
                else:
                    logger.error(f'WU-> NO DATA DETECTED FOR: {dbtable_schema}.{dbtable_name}')

        logger.info(f'WU -> DB PERSISTING PROCESS START!')

        for k,df in delivery_dict.items():
            input_df = df.copy()
            saves_df_intodb(conn_name, conns_str, input_df, k, schema_name, option)

        logger.info(f'WU -> DB PERSISTING PROCESS FINISHED!')