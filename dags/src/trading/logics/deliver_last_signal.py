import pandas as pd    # For data manipulation
import numpy as np
import datetime
from datetime import timedelta
import pytz

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from helpers.additional_functionalities import try_execution


class deliverLastSignal:

    def __init__(self, **kwargs):

        self.inputs_dict = kwargs.get('inputs_dict',None)
        params_dict = kwargs.get('params_dict',None)

        try:
            self.strategy = params_dict['strategy']
        except:
            self.strategy = None

    @try_execution
    def run(self):

        self.outputs_dict = self.compute_last_signal(self.inputs_dict)

    @try_execution
    def compute_last_signal(self, inputs_dict):

        if self.strategy in ['sp500_15m','sp500_15m_v2','sp500_15m_v3']:
        
            outputs_dict = inputs_dict
            data_e_df = outputs_dict['data_e_df'].copy()

            icols_ls = [
                'Datetime',
                'Close',
                'SMA1',
                'slope1',
                'signal',
                'strategy_gain',
                'acum_strategy_gain',
                'Cumulative Market Return',
                'Cumulative Strategy Return',
            ]

            last_signals_df = data_e_df[icols_ls].iloc[-4:,:].copy()
            last_signals_df = last_signals_df.reset_index(drop=True).copy()

            last_signals_df['prev_signal'] = last_signals_df['signal'].shift(1)

            last_signals_df.loc[
                last_signals_df['prev_signal'].isnull()
                ,'prev_signal'] = last_signals_df['signal']

            last_signals_df['Action'] = 'NA'

            last_signals_df.loc[
                (last_signals_df['prev_signal']!=last_signals_df['signal'])
                & (last_signals_df['signal'] == 1)
                ,'Action'] = 'BUY'

            last_signals_df.loc[
                (last_signals_df['prev_signal']!=last_signals_df['signal'])
                & (last_signals_df['signal'] == 0)
                ,'Action'] = 'CLOSE'

            last_signals_df['Strategy'] = self.strategy

            # Last signal
            max_stock_dt = max(data_e_df['Datetime'])
            max_stock_date = max_stock_dt.date()
            max_stock_time = str(max_stock_dt.hour).zfill(2) + str(max_stock_dt.minute).zfill(2)

            current_dt = datetime.datetime.now(datetime.timezone.utc)
            current_date = current_dt.date()
            current_time = str(current_dt.hour).zfill(2) + str(current_dt.minute).zfill(2)

            limit_dt = datetime.datetime(max_stock_date.year, max_stock_date.month, max_stock_date.day, 20, 15)
            utc=pytz.UTC
            limit_dt = limit_dt.replace(tzinfo=utc)

            logger.info(f'\nWU -> Last stock date: {max_stock_dt}\nLimit date: {limit_dt}\nCurrent date: {current_dt}\n')

            last_signal_df = last_signals_df.iloc[[-2]]
            last_signal_df['Market Status'] = 'OPEN'
            
            if (max_stock_date == current_date) & (2000 <= int(current_time) < 2015): #At 2000
                last_signal_df = last_signals_df.iloc[[-1]]
                last_signal_df['Market Status'] = 'LAST OPERATION'

            if current_dt > limit_dt:
                last_signal_df = last_signals_df.iloc[[-1]]
                last_signal_df['Market Status'] = 'CLOSED'
                

            last_signal_df = last_signal_df.reset_index(drop=True).copy()

            last_signal_df['Datetime'] = last_signal_df['Datetime'].apply(lambda x: str(x))

            dcols_ls = [
                'signal',
                'prev_signal'
            ]

            last_signal_df = last_signal_df.drop(columns=dcols_ls).copy()

            # Build last signal report
            last_signal = last_signal_df.T.reset_index().copy()
            rencols_dict = {
                'index':'Descripción',
                0:'Valor',
            }
            last_signal = last_signal.rename(columns=rencols_dict).copy()

            logger.info(f'\nWU -> Last signal report:\n{last_signal}\n')

            outputs_dict['last_signals_df'] = last_signals_df.copy()
            outputs_dict['last_signal_df'] = last_signal_df.copy()
            outputs_dict['last_signal'] = last_signal.copy()

        return outputs_dict