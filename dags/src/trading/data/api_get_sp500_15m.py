import yfinance as yf  # For stock data
import pandas as pd    # For data manipulation
import time

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)



class apiGetStockData:

    def __init__(self):

        # Define parameters
        self.ticker = "^GSPC"  
        self.interval = "15m"
        self.period = "60d"
        #self.start_date = "2025-01-01"
        #self.end_date = "2025-06-01"

    def run(self):

        self.outputs_dict = self.get_data()
        self.outputs_dict = self.data_arrangement(self.outputs_dict)

    def get_data(self):

        # Fetch historical stock data
        time.sleep(5) #Wait for 5 secs to passt complete the time interval

        stock_df = yf.download(self.ticker, interval=self.interval, period=self.period)
        #stock_df = yf.download(self.ticker,, interval="1d", start="2024-01-01", end="2024-03-31")

        stock_df = stock_df.reset_index()
        stock_df = stock_df.reset_index()

        stock_df.columns = [c[0] for c in stock_df.columns]

        # Display first few rows
        logger.info(f'\nWU -> Stock market data: {stock_df.shape}')
        #print(stock_df.tail(30))

        outputs_dict = {}
        outputs_dict['stock_df'] = stock_df.copy()

        return outputs_dict

    def data_arrangement(self, inputs_dict):

        outputs_dict = inputs_dict

        stock_df = outputs_dict['stock_df'].copy()

        data_e2_df = stock_df.copy()

        data_e2_df['Date'] = data_e2_df['Datetime'].apply(lambda x: x.date())
        data_e2_df['Time'] = data_e2_df['Datetime'].apply(lambda x: str(x.hour).zfill(2) + str(x.minute).zfill(2))

        # Adjusting Open price
        data_e2_df['Open_adj'] = data_e2_df['Close'].shift(1)

        obs_df = data_e2_df.groupby(
            ['Date'],
            as_index = False
        ).agg(
            candles = ('index','count'),
            min_datetime = ('Datetime','min'),
            max_datetime = ('Datetime','max'),
        )

        obs_df['time'] = obs_df['max_datetime'] - obs_df['min_datetime']

        logger.info(f'\nWU -> Last reported dates into the data:\n{obs_df.tail(7)}')

        outputs_dict['data_e2_df'] = data_e2_df.copy()

        return outputs_dict