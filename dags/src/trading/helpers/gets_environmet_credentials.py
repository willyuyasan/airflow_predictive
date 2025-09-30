import os
import time

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from helpers.app_environment_parameters import ENV_VARS_LS
from helpers.additional_functionalities import try_execution


class Getsenvironmentcredentials:

    def __init__(self):

        self.classname = type(self).__name__

    @try_execution
    def run(self):

        environment_vars_list, environment_vars_dict = self.get_environmental_vars_ls()
        conns_str = self.builds_conns_str(environment_vars_list, environment_vars_dict)
        
        return conns_str

    def get_environmental_vars_ls(self):

        environment_vars_list = []
        environment_vars_dict = {}

        try:
            with open(".env", "r") as f:
                env_file = f.readlines()

            for line in env_file:
                line_ls = line.split('=')
                if line_ls[0] != '\n':
                    environment_vars_list.append(line_ls[0].strip())
                    environment_vars_dict[line_ls[0].strip()] = str(line_ls[1]).replace('\n','').replace('"','')

            logger.info('WU-> ENV VARS TAKEN FROM .ENV FILE')

        except Exception as e:
            environment_vars_list = ENV_VARS_LS
            logger.info('WU-> ENV VARS TAKEN FROM SYSTEM')
            
        return environment_vars_list, environment_vars_dict

    def builds_conns_str(self, environment_vars_list, environment_vars_dict):
    
        conns_str = {}
        for environment_var in environment_vars_list:
        
            db_value_ls = environment_var.split('_')

            if db_value_ls !=['\n']:

                db = db_value_ls[0].lower()

                feature = db_value_ls[1:]
                feature = '_'.join(str(x).lower() for x in feature)

                if 'storage' in feature:
                    db = db + '_storage'
                    feature = feature.replace('storage_','')

                #Creates the dict key
                try:
                    conns_str[db]
                except:
                    conns_str[db] = {}

                #Adding features
                try:
                    conns_str[db][feature] = environment_vars_dict[environment_var]
                except:
                    conns_str[db][feature] = os.getenv(environment_var)
            else:
                pass

        return conns_str

