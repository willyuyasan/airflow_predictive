import pandas as pd    # For data manipulation
import numpy as np
import statsmodels.api as sm

import logging
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)



class computeStrategiesIndicators:

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

        # Important step to order the series and compute indicators
        data_e2_df = data_e2_df.sort_values('Datetime').copy()
        data_e2_df = data_e2_df.reset_index(drop=True).copy()

        # Window indicators SMA
        data_e2_df['SMA1'] = data_e2_df['Close'].rolling(window=100).mean()
        data_e2_df['SMA2'] = data_e2_df['Close'].rolling(window=200).mean()


        # Calculate daily percentage change in stock prices
        data_e2_df['current_return'] = data_e2_df['Close'].pct_change()


        # Difference with sma
        data_e2_df['sma1_diff'] = data_e2_df['Close'] - data_e2_df['SMA1']

        diff_df = data_e2_df[data_e2_df['sma1_diff'].isnull()==False][['sma1_diff']]
        diff_df['sma1_diff_bucket'] = pd.qcut(diff_df['sma1_diff'],11, labels=False)

        diff_df

        obs_df = diff_df.groupby(
            ['sma1_diff_bucket'],
            as_index=False
        ).agg(
            min_diff = ('sma1_diff','min'),
            max_diff = ('sma1_diff','max'),
            avg_diff = ('sma1_diff','mean'),
            frequence = ('sma1_diff','count'),
        ).copy()

        logger.info(f'\nWU -> Report of differences with SMA1:\n{obs_df}\n')


        ## Compute slope indicator
        slope_ls = [0]

        def slope_f(input_df, col, cicles):

            input_df = input_df.reset_index(drop=True).copy()

            datapoints_df = input_df[[col]].copy()

            for p in range(0, cicles):
                col_name = 'y'+str(p)
                datapoints_df[col_name] = input_df[col].shift(p).tolist()

            def compute_slope_f(x, cicles):

                slope = None
                X1 = []
                y = []
                
                try:
                    for q in range(0,cicles):
                        col_name = 'y'+str(cicles - (q+1))
                        y.append(x[col_name])
                        X1.append(q)
                    
                    X1 = np.array(X1)
                    y = np.array(y)
                    X1 = sm.add_constant(X1) # Adds a constant column for the intercept
                    model = sm.OLS(y, X1)
                    results = model.fit()
                    
                    slope = results.params.tolist()[1]
                    
                except Exception as e:
                    print(e)
                    pass
            
                return slope

            datapoints_df['slope'] = datapoints_df.apply(lambda x: compute_slope_f(x, cicles), axis=1)

            return datapoints_df['slope'].tolist()
            
        #datapoints_df = slope_f(data_e2_df, 'Close', 3)
        #datapoints_df

        data_e2_df['slope1'] = slope_f(data_e2_df, 'Close', 4)
        data_e2_df['slope2'] = slope_f(data_e2_df, 'Close', 5)

        slopes_df = data_e2_df[data_e2_df['slope1'].isnull()==False][['slope1']]
        slopes_df['slope_bucket'] = pd.qcut(slopes_df['slope1'],11, labels=False)

        slopes_df

        obs_df = slopes_df.groupby(
            ['slope_bucket'],
            as_index=False
        ).agg(
            min_s= ('slope1','min'),
            max_s = ('slope1','max'),
            avg_s = ('slope1','mean'),
            frequence = ('slope1','count'),
        ).copy()

        logger.info(f'\nWU -> Report of slopes values:\n{obs_df}\n')


        ## Compute tendency indicator
        prev_slope_ls = [0]
        tendency_ls = [0]
        acceleration_ls = [0]

        def tendency_f(x):
            slope = x['slope1']
            prev_slope = prev_slope_ls[0]
            tendency = tendency_ls[0]
            acceleration = acceleration_ls[0]

            if (slope<0):
                
                if tendency > 0:
                    tendency = 0
                    
                if (slope < prev_slope):

                    if acceleration < 0:
                        acceleration = 0
                    
                    tendency = tendency - 1
                    acceleration = acceleration + 1

                if (slope > prev_slope):

                    if acceleration > 0:
                        acceleration = 0

                    acceleration = acceleration - 1

            if (slope>0):

                if tendency < 0:
                    tendency = 0
                
                if (slope >= prev_slope):

                    if acceleration < 0:
                        acceleration = 0
                    
                    tendency = tendency + 1
                    acceleration = acceleration + 1

                if (slope < prev_slope):

                    if acceleration > 0:
                        acceleration = 0

                    acceleration = acceleration - 1

            tendency_ls[0] = tendency
            prev_slope_ls[0] = slope
            acceleration_ls[0] = acceleration

            return tendency, acceleration

        info = data_e2_df.apply(lambda x: tendency_f(x), axis=1)
        icols_ls = ['tendency', 'acceleration']
        info_df = pd.DataFrame(info.tolist(), columns=icols_ls)

        data_e2_df.loc[:,icols_ls] = info_df.loc[:,icols_ls]

        outputs_dict['data_e2_df'] = data_e2_df.copy()

        return outputs_dict
    
    def strategy_sp500_15m_v2(self, inputs_dict):

        outputs_dict = inputs_dict
        data_e2_df = outputs_dict['data_e2_df'].copy()

        # Important step to order the series and compute indicators
        data_e2_df = data_e2_df.sort_values('Datetime').copy()
        data_e2_df = data_e2_df.reset_index(drop=True).copy()

        # Compute Market gain
        data_e2_df['market_gain'] = data_e2_df['Open_adj'] - data_e2_df['Close'] 

        # Window indicators SMA
        data_e2_df['SMA1'] = data_e2_df['Close'].rolling(window=100).mean()
        data_e2_df['SMA2'] = data_e2_df['Close'].rolling(window=200).mean()

        #Normalize close price
        def normalize_close_f(x):
            close = x['Close']
            sma = x['SMA1']

            try:
                nclose = (close - sma)/sma
            except:
                nclose = None
            
            return nclose

        data_e2_df['nclose'] = data_e2_df.apply(lambda x: normalize_close_f(x), axis=1)

        # Calculate daily percentage change in stock prices
        data_e2_df['current_return'] = data_e2_df['Close'].pct_change()

        ## Compute slope indicator
        slope_ls = [0]

        def slope_f(input_df, col, cicles):

            input_df = input_df.reset_index(drop=True).copy()

            datapoints_df = input_df[[col]].copy()

            for p in range(0, cicles):
                col_name = 'y'+str(p)
                datapoints_df[col_name] = input_df[col].shift(p).tolist()

            def compute_slope_f(x, cicles):

                slope = None
                X1 = []
                y = []
                
                try:
                    for q in range(0,cicles):
                        col_name = 'y'+str(cicles - (q+1))
                        y.append(x[col_name])
                        X1.append(q)
                    
                    X1 = np.array(X1)
                    y = np.array(y)
                    X1 = sm.add_constant(X1) # Adds a constant column for the intercept
                    model = sm.OLS(y, X1)
                    results = model.fit()
                    
                    slope = results.params.tolist()[1]
                    
                except Exception as e:
                    print(e)
                    pass
            
                return slope

            datapoints_df['slope'] = datapoints_df.apply(lambda x: compute_slope_f(x, cicles), axis=1)

            return datapoints_df['slope'].tolist()
            
        #datapoints_df = slope_f(data_e2_df, 'Close', 3)
        #datapoints_df

        data_e2_df['slope1'] = slope_f(data_e2_df, 'nclose', 4)

        slopes_df = data_e2_df[data_e2_df['slope1'].isnull()==False][['slope1']]
        slopes_df['slope_bucket'] = pd.qcut(slopes_df['slope1'],11, labels=False)

        slopes_df

        obs_df = slopes_df.groupby(
            ['slope_bucket'],
            as_index=False
        ).agg(
            min_s= ('slope1','min'),
            max_s = ('slope1','max'),
            avg_s = ('slope1','mean'),
            frequence = ('slope1','count'),
        ).copy()

        logger.info(f'\nWU -> Report of slopes values:\n{obs_df}\n')


        ## Compute tendency indicator
        prev_slope_ls = [0]
        tendency_ls = [0]
        acceleration_ls = [0]

        def tendency_f(x):
            slope = x['slope1']
            prev_slope = prev_slope_ls[0]
            tendency = tendency_ls[0]
            acceleration = acceleration_ls[0]

            if (slope<0):
                
                if tendency > 0:
                    tendency = 0
                    
                if (slope < prev_slope):

                    if acceleration < 0:
                        acceleration = 0
                    
                    tendency = tendency - 1
                    acceleration = acceleration + 1

                if (slope > prev_slope):

                    if acceleration > 0:
                        acceleration = 0

                    acceleration = acceleration - 1

            if (slope>0):

                if tendency < 0:
                    tendency = 0
                
                if (slope >= prev_slope):

                    if acceleration < 0:
                        acceleration = 0
                    
                    tendency = tendency + 1
                    acceleration = acceleration + 1

                if (slope < prev_slope):

                    if acceleration > 0:
                        acceleration = 0

                    acceleration = acceleration - 1

            tendency_ls[0] = tendency
            prev_slope_ls[0] = slope
            acceleration_ls[0] = acceleration

            return tendency, acceleration

        info = data_e2_df.apply(lambda x: tendency_f(x), axis=1)
        icols_ls = ['tendency', 'acceleration']
        info_df = pd.DataFrame(info.tolist(), columns=icols_ls)

        data_e2_df.loc[:,icols_ls] = info_df.loc[:,icols_ls]

        outputs_dict['data_e2_df'] = data_e2_df.copy()

        return outputs_dict