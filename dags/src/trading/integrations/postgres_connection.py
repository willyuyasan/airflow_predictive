import os
import psycopg2
from sqlalchemy import create_engine
import json
import time
import pandas as pd
import numpy as np

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from helpers.additional_functionalities import try_execution


class postgresConnection:
    
    def __init__(self, name:str, conns_str=None, **kwargs):

        self.name = name
        self.conns_str = conns_str
        self.connection = kwargs.get('connection', None)
        
    @try_execution
    def run(self):

        self.defineconnection(self.name, self.conns_str, connection=self.connection),

    def defineconnection(self, name, conns_str, **kwargs):

        connection = kwargs.get('connection', None)

        if connection==None:
        
            if conns_str==None:
                # Defines the storage directories
                DIR = os.getcwd()
                #DIR = os.getcwd() + '/utils'

                #Imports connections string
                f = open(DIR + '/connections_strings.json')
                conns_str = json.load(f)
                f.close()

            conn_str = conns_str[name]
            
            user=conn_str['user']
            password=conn_str['password']
            hostname=conn_str['host']
            port = conn_str['port']
            database_name=conn_str['database']
            
            engine = create_engine(
                f'postgresql+psycopg2://{user}:{password}@{hostname}:{port}/{database_name}',
                connect_args={'connect_timeout': 10}
                )
            
            connection = engine.connect()

            connection2 = psycopg2.connect(
                host=conn_str['host'],
                database=conn_str['database'],
                user=conn_str['user'],
                password=conn_str['password'],
                port=conn_str['port'],
            )

            self.connection = connection
            self.connection2 = connection2
            self.engine = engine
            
            return connection, connection2
    
    def conn_testing(self, connection2=None, conn_name=None):

        if connection2==None:
            connection2 = self.connection2
            conn_name = self.name

        #Testing the connection
        try:
            cur = connection2.cursor()
            #print('PostgreSQL database version:') 
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            #print(db_version)
            cur.close()

            logger.info(f'##### Connection to ({conn_name}) DB sucessfully! #####')
        
        except Exception as e:
            logger.error(f'##### Connection to ({conn_name}) DB FAILED! ##### {e}')
            raise

    def get_mandatory_columns(self, schema, table_name, conn):

            dbtablecols_query = """
            SELECT 
            icols.column_name
            FROM information_schema.columns icols
            WHERE 
            table_schema = '{schema}'
            AND table_name   = '{table_name}'
            """

            query = dbtablecols_query.format(schema=schema, table_name=table_name)
            mandatorycols_df= pd.read_sql(query, con=conn)
            conn.close()

            return mandatorycols_df
    
    def dftodb(self, **kwargs):

        table_df = kwargs.get('table_df', None)
        table_dbname = kwargs.get('table_dbname', None)
        schema = kwargs.get('schema', 'public')
        option = kwargs.get('option', 'append')

        logger.info(f'#### ({table_dbname}) TABLE UPLOADING TO DB STARTED ####')
        
        if option=='append':
            self.tableappend(table_df, table_dbname, schema)
            
        elif option=='truncate':
            self.tabletruncation(table_df, table_dbname, schema)
            
        elif option=='creation':
            self.tablecreation(table_df, table_dbname, schema)
            
        else:
            raise ValueError(f'##### ({table_dbname}) NOT SAVED ON SP #####')
            
        logger.info(f'#### ({table_dbname}) TABLE UPLOADING TO DB FINISHED ####')
        
        
    def tablecreation(self, table_df, table_dbname, schema):
        
        con = self.connection

        table_df.to_sql(table_dbname, schema=schema, con=con, index=False, if_exists='replace')
        query = f'GRANT ALL ON  {schema}.{table_dbname} TO PUBLIC'
        con.execution_options(autocommit=True).execute(query)
        con.close()

        logger.info(f'WU -> Table created successfully!')
        
    def tableappend(self, table_df, table_dbname, schema):
        
        con = self.connection
        
        table_df.to_sql(table_dbname, schema=schema, con=con, index=False, if_exists='append')
        logger.info(f'WU -> Table appended successfully!')
        
    def tabletruncation(self, table_df, table_dbname, schema):
        
        con = self.connection 
        
        try:
            query = f'TRUNCATE TABLE {schema}.{table_dbname}'
            con.execution_options(autocommit=True).execute(query)
            time.sleep(1)
            logger.info(f'WU -> Table truncated successfully!')
        except Exception as e:
            logger.info(f'WU -> ERROR WHILE TRUNCATION: {e}')
            pass
        
        table_df.to_sql(table_dbname, schema=schema, con=con, index=False, if_exists='append')



# Aditional functionalities
    
def testing_connections_list(conns_names_ls, conns_str):

    logger.info(f'##### Testing connections list: #####')

    for conn_name in conns_names_ls:

        try:
            get_connection = Get_postgres_connection(conn_name, conns_str)
            get_connection.run()
            get_connection.conn_testing()

        except Exception as e:
            logger.error(f'##### Error trying to connect to ({conn_name})! ##### {e}')
            #raise

    logger.info(f'##### FOLLOWING CONNECTIONS TO DB SUCCESSFULLY TESTED: #####')
    logger.info(conns_names_ls)
