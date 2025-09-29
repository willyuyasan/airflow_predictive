import os
import pandas as pd
import numpy as np
import logging

from helpers.additional_functionalities import try_execution
from integrations.gd_connection import gdConnection

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)



class getConnsStr:

    def __init__(self, **kwargs):

        self.app = kwargs.get('app',None)
        self.app_environment = kwargs.get('app_environment',None)

    @try_execution
    def run(self):

        self.outputs_dict = self.get_connection_app_info()
        self.outputs_dict = self.get_connection_df(self.outputs_dict, self.app, self.app_environment)
        self.outputs_dict = self.create_conns_str(self.outputs_dict)
    
    @try_execution
    def get_connection_app_info(self):

        gdconnection = gdConnection(credential_filename='google_credentials.json')
        gdconnection.run()

        # Google drive url where the credentials excel file are located
        downloadfile_url = 'https://docs.google.com/spreadsheets/d/1XbMFRuieokdJHMj0DKNZOqDt5cYfAurI/edit?usp=drive_link&ouid=101376504031888677357&rtpof=true&sd=true'
        sheets_name_ls = ['credentials','app_connections']

        files_dict = gdconnection.download_excel_file(downloadfile_url, sheets_name_ls)
        credentials_df = files_dict['credentials'].copy()
        appconns_df = files_dict['app_connections'].copy()

        outputs_dict = {}
        outputs_dict['credentials_df'] = credentials_df.copy()
        outputs_dict['appconns_df'] = appconns_df.copy()
        return outputs_dict

    @try_execution
    def get_connection_df(self, inputs_dict, app, app_environment):

        outputs_dict = inputs_dict
        credentials_df = outputs_dict['credentials_df'].copy()
        appconns_df = outputs_dict['appconns_df'].copy()

        fappconns_df = appconns_df[
            (appconns_df['app']==app)
            &(appconns_df['app_environment']==app_environment)
        ].copy()

        os.environ['APP_ENV'] = app_environment
        logger.info(f'WU -> CURRENT WORKING ENVIRONMENT: {app_environment}')

        fcredentials_df = pd.merge(
            fappconns_df[['connection','credential','environment']],
            credentials_df,
            on = ['credential','environment'],
            how = 'inner'
        ).copy()

        outputs_dict = {}
        outputs_dict['fcredentials_df'] = fcredentials_df.copy()
        return outputs_dict
    
    
    def create_conns_str(self, inputs_dict):

        outputs_dict = inputs_dict
        credentials_2_df = outputs_dict['fcredentials_df'].copy()

        #Environment vars
        credentials_2_df.loc[:,'environment_var'] = credentials_2_df['connection'].str.upper() + '__' + credentials_2_df['feature'].str.upper()
        connections_ls = np.unique(np.array(credentials_2_df['connection'].tolist())).tolist()

        logger.info(f'WU ->  CONNECTION LIST:')
        for con in connections_ls:
            logger.info(f'{con}')

        #Builds conns_str and env file
        conns_str = {}
        env_file = ''
        env_ls = []
        for con in connections_ls:
            conns_str[con]={}

            credential_df = credentials_2_df[
                credentials_2_df['connection'] == con
            ].copy()

            for idx, row in credential_df.iterrows():
                conns_str[con][row['feature']] = row['value']
                value = row['value']

                env_file = env_file + row['environment_var'] + f'="{value}"\n'
                env_ls.append(row['environment_var'])
                
            env_file = env_file + '\n'

        outputs_dict['connections_ls'] = connections_ls
        outputs_dict['conns_str'] = conns_str
        outputs_dict['env_file'] = env_file
        outputs_dict['env_ls'] = env_ls
        return outputs_dict