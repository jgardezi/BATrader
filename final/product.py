# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 15:13:29 2018

@author: Branson
"""
import BATrader as ba
from collections import defaultdict

import pandas as pd
import numpy as np
import threading

# =============================================================================
#     Good implementation of products
# =============================================================================
"""
2019-02 Actually we don't need the product to have ability to inspect itself
"""


class BaseClassProducts(dict):
    """
    Derived from dictionary
    """

    # Public
    # symlist_index = defaultdict(list)
    # quotes        = defaultdict(list)

    @classmethod
    def find_by_sym(cls, sym):
        '''
        Using to find and return the instances
        '''
        return cls.symlist_index[sym][0]

    @classmethod
    def check_sym_instances(cls, sym):
        '''
        Return true if it is a instance of a 'sym'
        '''
        return sym in cls.symlist_index

    @classmethod
    def return_quotes(cls):
        '''
        Return the quotes of the class
        '''
        return cls.quotes

    def shift(self, shift: int):
        if shift > 0:
            self.shifted_daybar = self.shifted_daybar.append(self.daybar[-shift:], sort=True)
            self.daybar = self.daybar[:-shift]

    def shift_by_day(self, day):
        self.shifted_daybar = self.shifted_daybar.append(self.daybar[day:], sort= True)
        self.daybar = self.daybar[:day]
        #self.shifted_data1min = self.shifted_data1min.append(self.data1min[ba.dk.format_add_hyphen(day)])
        #self.data1min = self.data1min[:ba.dk.format_add_hyphen(day)]


class Stock(BaseClassProducts):
    """
    Pervious we use loaded_data to prevent dump from loading more than one time.
    But actually we may want to keep that.(dump will load again) in case some data
    updated to sql first and get it out again (like mmi)
    
    MemoryDict is running dump only once. Because it call __missing__, so it's safe
    to remove self.loaded_data. (2019-06-30)
    """
    symlist_index = defaultdict(list)
    quotes = defaultdict(dict)  # for storing the latest quotes when update

    # loaded data meta 預先知道什麼Data被load了,跟著下面更新
    meta = ['daybar', 'data1min']

    def __init__(self, sym):

        # BaseClass_Products.__init__(self)

        self.sym = sym
        self.chi_name = ba.fr.get_chi_name(sym)
        Stock.symlist_index[sym].append(self)
        
        # Scan meta : sometimes we need to put some scan meta data to Stock obj
        self.scanner_meta_data = {}
        self.shifted_daybar = pd.DataFrame()
        self.shifted_data1min = pd.DataFrame()

    @classmethod
    def find_by_sym(cls, sym):
        return Stock.symlist_index[sym][0]

    def display(self):
        print("Symbol:", self.sym)
        print("no. of EOD bar:", len(self.daybar))
        print("no. of 1min bar:", len(self.data1min))

    def dump(self, recalc= True, dayback= None):
        """
        load some dump data
        
        daybar and data1min is loaded from DB
        
        recalc:
            True will using tc recalc
        dayback:
            just load dayback data, must come with recalc= False
        
        """
        print('Dumping : %s' % self.sym)

        # Name
        setattr(self, 'name', ba.fr.get_name(self.sym))
        setattr(self, 'chi_name', ba.fr.get_chi_name(self.sym))

        # OHLC
        if recalc:
            setattr(self, 'daybar', ba.tc.get_bar_ex_eod_db_recalc_with_meta(self.sym))
        else:
            if dayback:
                setattr(self, 'daybar', ba.tc.get_bar_ex_eod_db(self.sym, dayback= dayback))
            else:
                setattr(self, 'daybar', ba.tc.get_bar_ex_eod_db(self.sym))
        setattr(self, 'data1min', ba.rtq.get_1min_sql(self.sym, dayback=30))



    def dump_meta_data(self):
        self.load_hkex_news()
        setattr(self, 'concat', pd.concat([self.daybar, self.mmi, self.details], sort=True))
        self.load_ccass()

    def dump_warrant_cbbc(self):

        if not hasattr(self, 'warrant'):
            self.warrant = ba.hsbc.get_warrant_group_by(self.sym)
        if not hasattr(self, 'cbbc'):
            self.cbbc = ba.hsbc.get_cbbc_group_by(self.sym)

    def load_hkexnews(self):
        """
        we always want this to be called once only. Since we can't backtest it easily.
        Just save some energy in daily running
        """
        if not hasattr(self, 'hkexnews'):
            setattr(self, 'hkexnews', ba.hkex.hkexnews_single_stock_news(self.sym))

    def load_ccass(self):
        setattr(self, 'ccass', ba.ccass.CCASS(self.sym))

    def load_mmi(self, dayback= 30):
        setattr(self, 'mmi', ba.mmi.get_by_sym_sql(self.sym, dayback= dayback))

    def load_details(self, dayback= 30, col_lst = []):
        setattr(self, 'details', ba.etnet.get_by_sym_sql(self.sym, dayback= dayback, col_lst= col_lst))
        
    def load_min_bar(self, min_: str):
        setattr(self, 'data%smin' % min_, 
                ba.algo.make_min_bar(self.data1min, '%sMin' % min_, simple_mode= True))
        
    def load_chi_name(self):
        setattr(self, 'chi_name', ba.fr.get_chi_name(self.sym))

    def _convert_tc_rtq_to_dataframe(self, dic):

        df = pd.DataFrame.from_dict(dic, orient='index').T

        df = pd.DataFrame.from_dict(dic, orient='index').T
        df['Date'] = pd.to_datetime(df['Date'], format="%Y%m%d")
        df['Open'] = df['Open'].astype(float)
        df['High'] = df['High'].astype(float)
        df['Low'] = df['Low'].astype(float)
        df['Close'] = df['Close'].astype(float)
        df['Vol'] = df['Vol'].astype(float)

        df = df.drop_duplicates(subset='Date', keep='last')[['Date', 'Open', 'High', 'Low', 'Close', 'Vol']].set_index(
            'Date')
        return df

    def calculate_meta(self):
        """
        daybar_chgp : The last day Chgp
        daybar_chg  : The last day Chg
        """
        try:
            self.daybar_chgp = round(((self.daybar.Close.iloc[-1] / self.daybar.Close.iloc[-2]) - 1) * 100, 2)
            self.daybar_chg = round((self.daybar.Close.iloc[-1] - self.daybar.Close.iloc[-2]), 2)
        except:
            self.daybar_chgp = round(self.daybar.Close.pct_change()[-1] * 100, 2)
            self.daybar_chg = round((self.daybar.Close - self.daybar.Close.shift(1))[-1], 2)

        
    def calculate_change(self):
        
        self.daybar = ba.algo.calc_chg_chgp(self.daybar)
        

    def update(self, data):
        """
        data is a dict, defined in pricer, get_stock_quote
        if adding field, need to change in the get_stock_quote function
        
        update:
            1. Return 5/15/30 mins bar
            2. Returns Week / Month bar
        """
        # print('Stock instance Updating : %s' % self.sym)
        # Less performance:
        # self.data1min = self.data1min.append(df).drop_duplicates(keep='last')

        # better performance:
        d = self.data1min.append(data['1min'])

        # 1min bar
        self.data1min = d[~d.index.duplicated(keep='last')]

        # daybar
        lastday = self.data1min.reset_index()["Datetime"].map(lambda t: t.date().strftime('%Y%m%d')).unique()[-1]
        resampled_daybar = ba.algo.make_day_bar(self.data1min[lastday])
        self.daybar = self.daybar.append(resampled_daybar, sort=True).reset_index().drop_duplicates(
            subset='Date', keep='last').set_index('Date')

        # calculate the meta
        self.calculate_meta()

    #        try:
    #            # debug or production, tc is getting in realtime, and append to the bar
    #            self.daybar = self.daybar.append(self._convert_tc_rtq_to_dataframe(data['tc'])).reset_index().drop_duplicates(subset='Date', keep='last').set_index('Date')
    #            #Stock.quotes[self.sym].update(data['tc'])
    #        except:
    #            # while in backtest, tc dict is empty, so we make the bar on our own
    #            self.daybar = self.daybar.append(ba.algo.make_day_bar(self.data1min)).reset_index().drop_duplicates(subset='Date', keep='first').set_index('Date')
    # Stock.quotes[self.sym].update(ba.algo.simulate_tc_dic(self.data1min))

    # For better performance, just calcuate the target stocks, but not
    # calc everytime when update
    #        setattr(self, 'data5min', ba.algo.make_min_bar(self.data1min, '5Min'))
    #        setattr(self, 'data30min', ba.algo.make_min_bar(self.data1min, '30Min'))
    #        setattr(self, 'weekbar', ba.algo.make_day_bar(self.daybar, 'W'))
    #        setattr(self, 'monthbar', ba.algo.make_day_bar(self.daybar, 'M'))

    def make_bar(self):
        """
        Sometimes we dont update, just need to make some bars
        """
        if not hasattr(self, 'daybar') and not hasattr(self, 'data1min'):
            print('No bar loaded')
            return
        else:
            print('Make bar')
            setattr(self, 'data5min', ba.algo.make_min_bar(self.data1min, '5Min'))
            setattr(self, 'data15min', ba.algo.make_min_bar(self.data1min, '15Min'))
            setattr(self, 'data30min', ba.algo.make_min_bar(self.data1min, '30Min'))
            setattr(self, 'weekbar', ba.algo.make_day_bar(self.daybar, 'W'))
            setattr(self, 'monthbar', ba.algo.make_day_bar(self.daybar, 'M'))

    def make_bar_threading(self):
        """
        Sometimes we dont update, just need to make some bars
        """
        if not hasattr(self, 'daybar') and not hasattr(self, 'data1min'):
            print('No bar loaded')
            return
        else:
            min_dic = {'data5min': '5Min',
                       'data15min': '15Min',
                       'data30min': '30Min'}
            day_dic = {'weekbar': 'W',
                       'monthbar': 'M'}

            def minbar_worker(dic):
                for k, v in dic.items():
                    setattr(self, k, ba.algo.make_min_bar(self.data1min, v))

            def daybar_worker(dic):
                for k, v in dic.items():
                    setattr(self, k, ba.algo.make_day_bar(self.daybar, v))

            print('Making bar by thread...')
            t1 = threading.Thread(target=daybar_worker, args=(day_dic,))
            t2 = threading.Thread(target=minbar_worker, args=(min_dic,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            print('Done.')

    def look(self):

        ba.algo.set_data(self.daybar)

        ba.algo.Signal_T3B()
        ba.algo.Signal_T3B2()
        ba.algo.Signal_BreakATR()
        ba.algo.Signal_MA_converge()
        ba.algo.Signal_Penetrate_ma60_ma200()
        ba.algo.Signal_Blow_BBands()

        print('================== T3B =====================')
        print(ba.algo.data['Signal_T3B'][ba.algo.data['Signal_T3B'] == True][-5:])
        print('================== T3B 2 =====================')
        print(ba.algo.data['Signal_T3B2'][ba.algo.data['Signal_T3B2'] == True][-5:])
        print('================== Break ATR =====================')
        print(ba.algo.data['Signal_BreakATR'][ba.algo.data['Signal_BreakATR'] == True][-5:])
        print('================== MA 5 10 20 =====================')
        print(ba.algo.data['Signal_MA_Converge'][ba.algo.data['Signal_MA_Converge'] == True][-5:])
        print('================== MA 60 200 =====================')
        print(ba.algo.data['Signal_penetrate_ma60_ma200'][ba.algo.data['Signal_penetrate_ma60_ma200'] == True][-5:])
        print('================== Blow BB =====================')
        print(ba.algo.data['Signal_Blow_BBands'][ba.algo.data['Signal_Blow_BBands'] == True][-5:])

    # ------ meta data ------
    def get_pe(self, latest=True, exact_date=''):
        value = ba.etnet.get_by_sym_sql(self.sym,
                                        exact_date=str(exact_date),
                                        col_lst=['P/E Ratio/Est.'])
        value['PE'] = value['PE'].astype(float)
        if latest:
            pe = value['PE'].iat[-1]
            setattr(self, 'PE', pe)
        else:
            pe = value['PE']
        return pe

    def get_pe_est(self, latest=True, exact_date=''):
        value = ba.etnet.get_by_sym_sql(self.sym,
                                        exact_date=str(exact_date),
                                        col_lst=['P/E Ratio/Est.'])
        value['PE_est'] = value['PE_est'].astype(float)
        if latest:
            pe_est = value['PE_est'].iat[-1]
            setattr(self, 'PE_est', pe_est)
        return pe_est

    def get_pb(self, exact_date=''):
        value = ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['net_book_value'])
        return value

    def get_vwap(self, exact_date=''):
        value = ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['VWAP'])
        return value

    def get_close(self, exact_date=''):
        value = ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['Last'])
        return value

    def get_tick(self, exact_date=''):
        return ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['tick'])

    def get_chg_chgp(self, exact_date=''):
        return ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['Chg', 'Chgp'])

    def get_10Dreturn(self, exact_date=''):
        return ba.etnet.get_by_sym_sql(self.sym, exact_date=str(exact_date), col_lst=['return_rate_10d'])

    def get_peg(self, sym):
        """
        假設公式 : 靜態市盈率 / 最近公怖的增長率
        """
        c = lambda x: float(x) if x != '' else -1
        growth = c(list(ba.analyst.aastocks_profit_loss(sym)['Net Profit Growth (%)'].values())[-1].replace('-', ''))
        pe = self.get_pe(self.sym, latest=True)

        PEG = pe / growth
        if PEG < 0:
            PEG = np.nan
        return PEG


class Commodities(BaseClassProducts):
    #    symlist_index = defaultdict(list)
    #    quotes        = defaultdict(dict)

    meta = ['daybar', 'data1min']

    def __init__(self, sym):

        #        BaseClass_Products.__init__(self)

        self.sym = sym

    #        Commodities.symlist_index[sym].append(self)

    def display(self):
        print("Symbol:", self.sym)
        print("no. of EOD bar:", len(self.daybar))
        print("no. of 1min bar:", len(self.data1min))

    def dump(self, recalc= True, dayback= None):

        print('Dumping : %s' % self.sym)

#            try:
        setattr(self, 'data1min', ba.f.get_futures_sql(self.sym, dayback=30))  # 不包含夜期
        setattr(self, 'daybar', ba.algo.make_day_bar(self.data1min))
#            except Exception as e:
#                print(str(e))
#                setattr(self, 'data1min', ba.f.get_aastocks_intraday_futures(self.sym))
#                setattr(self, 'daybar', ba.algo.make_day_bar(self.data1min))


    def _init_f(self):

        # init some main figures
        return dict(
            pClose=self.daybar.Close.iloc[-2],
            pHigh=self.daybar.High.iloc[-2],
            pLow=self.daybar.Low.iloc[-2],
            pOpen=self.daybar.Open.iloc[-2],
            pRange=self.daybar.High.iloc[-2] - self.daybar.Low.iloc[-2],
            pBody=abs(self.daybar.Close.iloc[-2] - self.daybar.Open.iloc[-2]),
            pVol=self.daybar.Vol.iloc[-2])

    def calculate_meta(self):

        self.daybar_chg = round((self.daybar.Close[-1] - self.daybar.Close.shift(1)[-1]), 2)

    def update(self, data):
        """
        data is a dict, defined in pricer, get_futures_quote
        if adding field, need to change in the get_futures_quote function
        """
        # print('Commodities instance Updating :', self.sym)

        ## better performance
        d = self.data1min.append(data['1min'])
        self.data1min = d[~d.index.duplicated(keep='last')]
        self.daybar = ba.algo.make_day_bar(self.data1min)

        self.calculate_meta()

        # Commodities.quotes[self.sym].update(data['summary'])

        # For better performance, just calcuate the target stocks, but not 
        # calc everytime when update
        # setattr(self, 'data5min', ba.algo.make_min_bar(self.data1min, '5Min'))
        # setattr(self, 'data30min', ba.algo.make_min_bar(self.data1min, '30Min'))

        # setattr(self, 'weekbar', ba.algo.make_day_bar(self.daybar, 'W'))
        # setattr(self, 'monthbar', ba.algo.make_day_bar(self.daybar, 'M'))


if __name__ == '__main__':
    #    f = Commodities('HSIF')
    #    f.dump()
    from BATrader.utils.toolbox import timeit
    
    s = Stock('620')
    s.dump()
#    s.calculate_meta()

#    from BATrader.final.scanner import thirty_minues_blow_bbands, five_minutes_three_in_row, is_today_t3b, \
#    is_today_break_atr, is_today_break_bb_daybar, is_today_over_ma_60_200
#    is_today_t3b(s)
    
    c = Commodities('HSIF')
    c.dump()
    
    
    
#    period_tuple = ('20190627', '20190628')

#    # Calculate the day range
#    rng = []
#    for day in ba.dk.date_ranger(ba.dk.get_next_calendar_day(period_tuple[0]),
#                                 ba.dk.get_next_calendar_day(period_tuple[1])):
#        for t in pd.date_range("%s 9:30" % day, "%s 16:30" % day, freq='min'):
#            if t not in pd.date_range("%s 12:01" % day, "%s 12:59" % day, freq='min'):
#                rng.append(t)