import pandas as pd    # For data manipulation
import numpy as np

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)



class strategiesImplementation:

    def __init__(self, **kwargs):

        self.strategy = kwargs.get('strategy',None)
        self.inputs_dict = kwargs.get('inputs_dict',None)


    def run(self):

        if self.strategy == 'sp500_15m':
            self.outputs_dict = self.strategy_sp500_15m(self.inputs_dict)

        if self.strategy == 'sp500_15m_v2':
            self.outputs_dict = self.strategy_sp500_15m_v2(self.inputs_dict)


    def strategy_sp500_15m(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e2_df = outputs_dict['data_e2_df'].copy()

        min_datetime = min(data_e2_df['Datetime'])
        max_datetime = max(data_e2_df['Datetime'])

        hist_months = (max_datetime - min_datetime).days / 30

        logger.info(f'\nWU-> Total data for computing strategy: {data_e2_df.shape},\nFrom: ({min_datetime}) to ({max_datetime})\nMonths: {hist_months}')

        signal_ls = [0]
        order_number_ls = [0]
        order_step_ls = [0]
        open_price_ls = [0]
        acum_strategy_gain_ls = [0] 
        order_gain_balance_ls = [0]
        order_loss_balance_ls = [0]

        def orders_steps_f(x):
            index = x['index']
            openn = x['Open_adj']
            close = x['Close']
            current_return = x['current_return']
            sma1 = x['SMA1']
            sma2 = x['SMA2']
            sma1_diff = x['sma1_diff']
            slope = x['slope1']
            tendency = x['tendency']
            time = int(x['Time'])

            signal = signal_ls[0]
            order_number = order_number_ls[0]
            order_step = order_step_ls[0]
            open_price = open_price_ls[0]
            acum_strategy_gain = acum_strategy_gain_ls[0]
            order_gain_balance = order_gain_balance_ls[0]
            order_loss_balance = order_loss_balance_ls[0]

            start_order = False

            
            # Estrategy signal
            if (signal==0):

                if (0.8 < slope) & (-5 < sma1_diff < 70):
                    signal = 1
                    start_order = True

                if (2 < slope < 10) & (-10 < sma1_diff < 0):
                    signal = 1
                    start_order = True

                if (sma1_diff < -20):
                    signal = 1
                    start_order = True

            # Order balance
            if start_order == True:
                order_step = 0
                order_number = order_number + 1
                open_price = close
                order_gain_balance = 0
                order_loss_balance = 0

            order_step = order_step + 1

            strategy_gain = (close - openn) * signal    
            strategy_return = current_return * signal

            if order_step == 1:
                strategy_gain = 0
                acum_strategy_gain = 0
                strategy_return = 0   
                
            if strategy_gain >= 0:
                order_gain_balance = order_gain_balance + strategy_gain

            if strategy_gain < 0:
                order_loss_balance = order_loss_balance + strategy_gain

            if acum_strategy_gain != 0:
                pp_change_gain = strategy_gain / acum_strategy_gain
            else:
                pp_change_gain = 0

            acum_strategy_gain = acum_strategy_gain + strategy_gain

            if open_price != 0:
                pp_strategy_gain = acum_strategy_gain / open_price
            else:
                pp_strategy_gain = 0     

            # Order closure conditions
            if (signal==1):

                if (acum_strategy_gain >= 30) | (acum_strategy_gain <= -25) :
                    signal = 0
                if (pp_change_gain < -0.3):
                    signal = 0
                if (slope < -10):
                    signal = 0
                if (tendency <= -3):
                    signal = 0
                if (acum_strategy_gain <0) & (order_step > 3):
                    signal = 0
                    
            signal_ls[0] = signal
            order_number_ls[0] = order_number
            order_step_ls[0] = order_step
            open_price_ls[0] = open_price
            acum_strategy_gain_ls[0] = acum_strategy_gain
            order_gain_balance_ls[0] = order_gain_balance
            order_loss_balance_ls[0] = order_loss_balance

            return signal, open_price, order_number, order_step, strategy_gain, pp_change_gain, order_gain_balance, order_loss_balance, strategy_return, acum_strategy_gain, pp_strategy_gain

        info = data_e2_df.apply(lambda x: orders_steps_f(x), axis=1)
        cols_ls = [
            'signal', 
            'open_price', 
            'order_number', 
            'order_step', 
            'strategy_gain', 
            'pp_change_gain', 
            'order_gain_balance', 
            'order_loss_balance',
            'strategy_return', 
            'acum_strategy_gain', 
            'pp_strategy_gain'
        ]

        info_df = pd.DataFrame(info.tolist(), columns=cols_ls).copy()

        data_e2_df.loc[:,cols_ls] = info_df.loc[:,cols_ls]

        outputs_dict['data_e2_df'] = data_e2_df.copy()

        return outputs_dict
    
    def strategy_sp500_15m_v2(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e2_df = outputs_dict['data_e2_df'].copy()

        signal_ls = [0]
        order_number_ls = [0]
        order_step_ls = [0]
        open_price_ls = [0]
        acum_strategy_gain_ls = [0] 
        max_strategy_gain_ls = [0]
        order_gain_balance_ls = [0]
        order_loss_balance_ls = [0]

        def orders_steps_f(x):
            index = x['index']
            openn = x['Open_adj']
            close = x['Close']
            nclose = x['nclose']
            current_return = x['current_return']
            slope = x['slope1']
            tendency = x['tendency']
            time = int(x['Time'])

            signal = signal_ls[0]
            order_number = order_number_ls[0]
            order_step = order_step_ls[0]
            open_price = open_price_ls[0]
            acum_strategy_gain = acum_strategy_gain_ls[0]
            max_strategy_gain = max_strategy_gain_ls[0]
            order_gain_balance = order_gain_balance_ls[0]
            order_loss_balance = order_loss_balance_ls[0]

            start_order = False

            # Estrategy signal
            if (signal==0):

                if (0.001 < slope) & (-0.004 < nclose):
                    signal = 1
                    start_order = True
                
                if (0.0005 < slope) & (-0.002 < nclose < 0.003):
                    signal = 1
                    start_order = True

                if (-0.0005 < slope) & (nclose < -0.004):
                    signal = 1
                    start_order = True
                
                if (slope > -0.000117) & (nclose > 0.01):
                    signal = -1
                    start_order = True

            # Order balance
            if start_order == True:
                order_step = 0
                order_number = order_number + 1
                open_price = close
                order_gain_balance = 0
                order_loss_balance = 0

            order_step = order_step + 1

            strategy_gain = (close - openn) * signal    
            strategy_return = current_return * signal

            if order_step == 1:
                strategy_gain = 0
                acum_strategy_gain = 0
                max_strategy_gain = 0
                strategy_return = 0   
                
            if strategy_gain >= 0:
                order_gain_balance = order_gain_balance + strategy_gain

            if strategy_gain < 0:
                order_loss_balance = order_loss_balance + strategy_gain
            
            acum_strategy_gain = acum_strategy_gain + strategy_gain
            max_strategy_gain = max([max_strategy_gain, acum_strategy_gain])

            if max_strategy_gain != 0:
                pp_max_gain = acum_strategy_gain / max_strategy_gain
            else:
                pp_max_gain = -1
                max_strategy_gain = strategy_gain

            # Order closure conditions
            if (signal==1):

                if (nclose > 0.009):
                    signal = 0

                if (pp_max_gain < 0.5) & (order_step > 3):
                    signal = 0

                if (acum_strategy_gain <0) & (order_step > 3):
                    signal = 0

                if (nclose < -0.005) & (acum_strategy_gain <0):
                    signal = 0
            
            if (signal==-1):

                if (nclose < -0.009):
                    signal = 0

                if (pp_max_gain < 0.8) & (order_step > 3):
                    signal = 0
                
                if (nclose > -0.0005) & (acum_strategy_gain <0):
                    signal = 0

                #if (acum_strategy_gain <= -10) | (acum_strategy_gain > 10):
                #    signal = 0
                    
            signal_ls[0] = signal
            order_number_ls[0] = order_number
            order_step_ls[0] = order_step
            open_price_ls[0] = open_price
            acum_strategy_gain_ls[0] = acum_strategy_gain
            max_strategy_gain_ls[0] = max_strategy_gain
            order_gain_balance_ls[0] = order_gain_balance
            order_loss_balance_ls[0] = order_loss_balance

            return signal, open_price, order_number, order_step, strategy_gain, pp_max_gain, order_gain_balance, order_loss_balance, strategy_return, acum_strategy_gain, max_strategy_gain

        info = data_e2_df.apply(lambda x: orders_steps_f(x), axis=1)
        cols_ls = [
            'signal', 
            'open_price', 
            'order_number', 
            'order_step', 
            'strategy_gain', 
            'pp_max_gain', 
            'order_gain_balance', 
            'order_loss_balance',
            'strategy_return', 
            'acum_strategy_gain', 
            'max_strategy_gain',
        ]

        info_df = pd.DataFrame(info.tolist(), columns=cols_ls).copy()

        data_e2_df.loc[:,cols_ls] = info_df.loc[:,cols_ls]

        outputs_dict['data_e2_df'] = data_e2_df.copy()

        return outputs_dict