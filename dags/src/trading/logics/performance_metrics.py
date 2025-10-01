import io
import pandas as pd    # For data manipulation
import numpy as np
import matplotlib.pyplot as plt  # For visualizations

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)



class performanceMetrics:

    def __init__(self, **kwargs):

        self.inputs_dict = kwargs.get('inputs_dict',None)
        params_dict = kwargs.get('params_dict',None)

        try:
            self.days = params_dict['days']
        except:
            self.days = None

        try:
            self.interval = params_dict['interval']
        except:
            self.interval = None

        try:
            self.ticker = params_dict['ticker']
        except:
            self.ticker = None

    def run(self):
        
        self.outputs_dict = self.select_data(self.inputs_dict, interval = self.interval, days = self.days)
        self.outputs_dict = self.computes_cummulative_returns(self.outputs_dict)
        self.outputs_dict = self.computes_strategy_gains(self.outputs_dict)
        self.operations_profit(self.outputs_dict)

    def select_data(self, inputs_dict, **kwargs):

        interval = kwargs.get('interval',None)
        days = kwargs.get('days',None)

        outputs_dict = inputs_dict
        data_e2_df = outputs_dict['data_e2_df'].copy()

        if (interval!=None) & (days!=None):

            if interval == '15m':
                nro_candles = days*26

            logger.info(f'\nWU -> intervals for compute performance of strategy:\nDays: {days}\ninterval: {interval}\n')

            data_e_df = data_e2_df.iloc[-nro_candles:,:].copy()

        else:

            data_e_df = data_e2_df.copy()


        data_e_df = data_e_df.drop(columns=['index']).copy()
        data_e_df = data_e_df.reset_index(drop=True).copy()
        data_e_df = data_e_df.reset_index().copy()

        data_e_df.loc[
            data_e_df['index']==0
            ,'current_return'] = 0

        data_e_df = data_e_df[data_e_df['order_number']>=1].copy()

        logger.info(f'\nWU -> Data for compute performance of strategy:\n{data_e_df.shape}\n')

        outputs_dict['data_e_df'] = data_e_df.copy()

        return outputs_dict
    
    def computes_cummulative_returns(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e_df = outputs_dict['data_e_df'].copy()

        # Calculate cumulative returns
        data_e_df.loc[
            data_e_df['order_number']==0
            , 'current_return'] = 0

        data_e_df['Cumulative Market Return'] = (1 + data_e_df['current_return']).cumprod()
            
        data_e_df['Cumulative Strategy Return'] = (1 + data_e_df['strategy_return']).cumprod()
        
        outputs_dict['data_e_df'] = data_e_df.copy()

        return outputs_dict
        
    def computes_strategy_gains(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e_df = outputs_dict['data_e_df'].copy()

        # Create a figure and a 1x2 grid of subplots (1 row, 2 columns)
        fig, axs = plt.subplots(2, 1, figsize=(14, 10)) # Adjust figsize as needed

        # Strategy gains

        #plt.figure(figsize=(14, 7))
        axs[0].plot(data_e_df['Cumulative Market Return'], label='Market Return', alpha=0.75)
        axs[0].plot(data_e_df['Cumulative Strategy Return'], label='Strategy Return', alpha=0.75)
        axs[0].set_title("Cumulative Returns")
        axs[0].legend(loc='upper left')
        #plt.show()

        total_strategy_return = data_e_df['Cumulative Strategy Return'].iloc[-1] - 1
        total_market_return = data_e_df['Cumulative Market Return'].iloc[-1] - 1

        logger.info(f"WU ->\nTotal Strategy Return: {total_strategy_return:.2%}\nTotal Market Return: {total_market_return:.2%}\n")

        timewindow = str(self.days) + ' days'

        #defining the attributes
        col_labels = ['Info','Value']
        row_labels = ['Total Strategy Return','Total Market Return','Time']
        table_vals = [[f'{total_strategy_return:.2%}'], [f'{total_market_return:.2%}'], [f'{timewindow}']]
        row_colors = ['orange', 'blue', None]
        #plotting
        my_table = axs[0].table(cellText=table_vals,
                            colWidths=[0.1] * 3,
                            rowLabels=row_labels,
                            colLabels=col_labels,
                            rowColours=row_colors,
                            loc='upper right')


        # Stock Series with indicators
        #plt.figure(figsize=(14, 7))
        axs[1].plot(data_e_df['Close'], label='Close Price', alpha=0.5)
        #axs[1].plot(data_e2_df['SMA20'], label='SMA20', alpha=0.75)
        axs[1].plot(data_e_df['SMA1'], label='SMA1', alpha=0.75)
        axs[1].plot(data_e_df['SMA2'], label='SMA2', alpha=0.75)
        axs[1].set_title(f"{self.ticker} Price and Moving Averages")
        axs[1].legend(loc='upper left')


        plt.savefig('Strategy_summary.png')

        imagebuffer = io.BytesIO()

        plt.savefig(imagebuffer, format='png')

        plt.show()
        plt.close()

        outputs_dict['imagebuffer'] = imagebuffer

        return outputs_dict
    
    def operations_profit(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e_df = outputs_dict['data_e_df'].copy()

        orders_df = data_e_df.loc[
            (data_e_df['signal'] == 0)
            &(data_e_df['strategy_gain'] == 0)==False
            ].copy()

        orders_df['order_candles'] = orders_df.groupby(['order_number'])['index'].transform('count')

        ordersgain_df = orders_df.groupby(['order_number'], as_index=False).last().copy()

        ordersgain_df['is_profitable'] = False

        ordersgain_df.loc[
            ordersgain_df['acum_strategy_gain']>0
            ,'is_profitable']=True

        obs_df = ordersgain_df.groupby(
            ['is_profitable'],
            as_index = False
        ).agg(
            oreders = ('order_number','count'),
            gain_amount = ('acum_strategy_gain','sum'),
            gain_avg = ('acum_strategy_gain','mean'),
            candles = ('order_candles','mean'),
        ).copy()

        logger.info(f'\nWU -> Operations profitability:\n{obs_df}\n')

        try:
            loss_amount = obs_df[obs_df['is_profitable']==False]['gain_amount'].iloc[0]
        except:
            loss_amount = 0

        try:
            gain_amount = obs_df[obs_df['is_profitable']==True]['gain_amount'].iloc[0]
        except:
            gain_amount = 0

        strategy_amount_gain = gain_amount + loss_amount

        logger.info(f'\nWU -> Strategy total gain points:({strategy_amount_gain})\n')
        
        init_price = data_e_df['Close'].iloc[1]
        final_price =data_e_df['Close'].iloc[-1]

        market_amount_gain = final_price - init_price
        
        logger.info(f'\nWU -> Market total gain points:({market_amount_gain})\n')

        
        

