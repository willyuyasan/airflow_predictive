import sys
import os
import io
import time
import pandas as pd

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


def try_execution(func):
    def wrapper(*args, **kwargs):

        start_time = time.time()
        #logger.info(f'WU-> Function ({func.__qualname__}) start!)')

        try:
            result = func(*args, **kwargs)
            elapsed_minutes = round((time.time() - start_time)/60,3)
            logger.info(f'\nWU-> Function ({func.__qualname__}) processed! (elapsed minutes: {elapsed_minutes})\n')
            return result
        except Exception as e:
            logger.error(f'\n\nWU-> Error in ({func.__qualname__}) class!\n{e}\n\n')
            raise

    return wrapper

def excel_buffer_f(input_df):

    document_buffer = io.BytesIO()
    writer_excel = pd.ExcelWriter(document_buffer, engine='xlsxwriter')
    input_df.to_excel(writer_excel, sheet_name='Hoja1', index=False)
    writer_excel.close()

    return document_buffer