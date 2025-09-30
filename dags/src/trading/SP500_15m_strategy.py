import sys
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta

from tabulate import tabulate

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

from helpers.get_conns_str import getConnsStr

from data.api_get_sp500_15m import apiGetStockData
from delivers.stockdata_todb import stockDataToDb
from data.stkdb_get_sp500_15m_hist import getSp50015mHist

from logics.compute_strategies_indicators import computeStrategiesIndicators
from logics.strategies_implementation import strategiesImplementation
from logics.performance_metrics import performanceMetrics
from logics.deliver_last_signal import deliverLastSignal

from integrations.whatsapp_connection import whatsappConnection
from integrations.telegram_connection import telegramConnection



class sp50015mStrategy:

    def __init__(self, **kwargs):

        self.strategy = 'sp500_15m'


    def run(self):
        
        self.outputs_dict = self.get_conns_str()
        self.outputs_dict = self.get_data(self.outputs_dict)
        self.outputs_dict = self.transformations(self.outputs_dict)

    def get_conns_str(self):

        getconnsstr = getConnsStr(app = 'trading', app_environment = 'PROD')
        getconnsstr.run()
        outputs_dict = getconnsstr.outputs_dict

        return outputs_dict

    def get_data(self, inputs_dict):

        outputs_dict = inputs_dict
        
        # Get last data from the api
        apigetstockdata = apiGetStockData()
        apigetstockdata.run()
        outputs2_dict = apigetstockdata.outputs_dict

        outputs_dict['data_e2_df'] = outputs2_dict['data_e2_df'].copy()

        self.ticker = apigetstockdata.ticker
        self.interval = apigetstockdata.interval
        self.period = apigetstockdata.period

        # Save latest obtained stock data
        params_dict = {}
        params_dict['stock_desc'] = 'sp500_15m'

        stockdatatodb = stockDataToDb(
            inputs_dict = outputs_dict, 
            params_dict = params_dict
        )

        stockdatatodb.run()

        # Get total historical stock data
        conns_str = outputs_dict['conns_str']
        getsp50015mhist = getSp50015mHist(conns_str = conns_str)
        getsp50015mhist.run()
        stock_df = getsp50015mhist.stock_df.copy()
        outputs_dict['data_e2_df'] = stock_df.copy()

        return outputs_dict
    
    def transformations(self, inputs_dict):

        outputs_dict = inputs_dict
        
        # Compute indicators
        strategy = self.strategy 
        computestrategiessndicators = computeStrategiesIndicators(strategy = strategy, inputs_dict = outputs_dict)
        computestrategiessndicators.run()
        outputs_dict = computestrategiessndicators.outputs_dict

        # Applay strategy
        strategiesimplementation = strategiesImplementation(strategy = strategy, inputs_dict = outputs_dict)
        strategiesimplementation.run()
        outputs_dict = strategiesimplementation.outputs_dict

        # Performance metrics
        params_dict = performace_metrics_params(self.ticker, self.interval)
        performancemetrics = performanceMetrics(inputs_dict = outputs_dict, params_dict=params_dict)
        performancemetrics.run()
        outputs_dict = performancemetrics.outputs_dict

        # Signal delivery
        params_dict = {}
        params_dict['strategy'] = strategy
        deliverlastsignal = deliverLastSignal(inputs_dict = outputs_dict, params_dict=params_dict)
        deliverlastsignal.run()
        outputs_dict = deliverlastsignal.outputs_dict

        return outputs_dict
    
    def send_notification(self, inputs_dict):

        last_signal_df = inputs_dict['last_signal_df'].copy()

        action = last_signal_df['Action'].iloc[0]
        market_status = last_signal_df['Market Status'].iloc[0]

        if action in ['BUY','SELL','CLOSE']:

            send_whatsapp_notification(inputs_dict)
            send_telegram_notification(inputs_dict)

        if market_status in ['CLOSED']:

            logger.info(f'\n\nWU -> MARKET CLOSED, PROCESS TERMINATED!\n\n')
            #sys.exit('WU -> MARKET CLOSED, PROCESS TERMINATED!')






# Additional functionalities

def performace_metrics_params(ticker, interval):
    current_dt = datetime.datetime.now(datetime.timezone.utc)
    current_date = current_dt.date()
    current_time = str(current_dt.hour).zfill(2) + str(current_dt.minute).zfill(2)

    params_dict = {}

    if (1315 <= int(current_time) <= 1959):
        params_dict['ticker'] = ticker
        params_dict['interval'] = interval
        params_dict['days'] = 7
    
    return params_dict
    
def send_whatsapp_notification(inputs_dict):

    outputs_dict = inputs_dict
    last_signal = outputs_dict['last_signal'].copy()
    imagebuffer = outputs_dict['imagebuffer']

    # Send notification to whatsapp
    last_signal_tab = tabulate(last_signal, headers = 'keys', tablefmt = 'pretty', showindex=False, colalign=("left", "right"))
    last_signal_tab = '```' + last_signal_tab + '```'

    whatsappconnection = whatsappConnection()
    recepient_phone_number = '573104888469'
    msg_body = last_signal_tab
    whatsappconnection.send_message(msg_body = msg_body, recepient_phone_number=recepient_phone_number)
    whatsappconnection.send_image(recepient_phone_number=recepient_phone_number, image_buffer = imagebuffer)

def send_telegram_notification(inputs_dict):

    outputs_dict = inputs_dict
    last_signal = outputs_dict['last_signal'].copy()
    imagebuffer = outputs_dict['imagebuffer']

    last_signal_tab = tabulate(last_signal, headers = 'keys', tablefmt = 'pretty', showindex=False, colalign=("left", "right"))
    message_text = '<pre>' + last_signal_tab + '</pre>'

    telegramconnection =telegramConnection()
    telegramconnection.send_image(imagebuffer=imagebuffer, message_text = message_text)

