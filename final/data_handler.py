# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 18:34:46 2019

@author: Branson
"""

from abc import ABCMeta, abstractmethod
import datetime
import os, os.path
import queue as queue

import numpy as np
import pandas as pd

from threading import Thread
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import BATrader as ba

from BATrader.final.event import MarketEvent, RefreshEvent
from BATrader.final.product import Stock, Commodities

from collections import OrderedDict, defaultdict
from enum import Enum

order_dic_format = OrderedDict({'time': '',
                                'buysell': '',
                                'sym': '',
                                'price': '',
                                'quantity': '',
                                'strength': '',
                                'product': '',
                                })

quote_dic = OrderedDict({'msg_type': 'data',
                         'product': '',
                         'sym': '',
                         'summary': '',
                         'tc': '',
                         '1min': ''})

msg_dic = OrderedDict({'msg_type': 'msg',
                       'msg_body': ''})



"""
dic['HKEXNEWS']['386'] = ''
"""

signal_dic = {'HKEXNEWS': {},
              '': {},
              }

params_dict = {'period': '',
               'dayback': 0,
               'startday': None,
               'endday': None}


# =============================================================================
# Sptrader
# =============================================================================
class FuturesMonthCode(Enum):
    Jan = 'F'
    Feb = 'G'
    Mar = 'H'
    Apr = 'J'
    May = 'K'
    Jun = 'M'
    Jul = 'N'
    Aug = 'Q'
    Sep = 'U'
    Oct = 'V'
    Nov = 'X'
    Dec = 'Z'


class CallOptionMonthCode(Enum):
    Jan = 'A'
    Feb = 'B'
    Mar = 'C'
    Apr = 'D'
    May = 'E'
    Jun = 'F'
    Jul = 'G'
    Aug = 'H'
    Sep = 'I'
    Oct = 'J'
    Nov = 'K'
    Dec = 'L'


class PutOptionMonthCode(Enum):
    Jan = 'M'
    Feb = 'N'
    Mar = 'O'
    Apr = 'P'
    May = 'Q'
    Jun = 'R'
    Jul = 'S'
    Aug = 'T'
    Sep = 'U'
    Oct = 'V'
    Nov = 'W'
    Dec = 'X'

# =============================================================================
# 
# =============================================================================
class MsgDict(Enum):
    msg_type = ''
    product = ''
    sym = ''


class MonitorSignal:
    """
    The old signal
    """
    header_msg = ''
    signal_msg = ''
    blowBB = False
    three_in_row = False
    T3B2 = False
    BreakATR = False
    over_ma60_ma200 = False
    blowBB_daybar = False


class FixedDict(OrderedDict):
    """
    Fixed Dict , cannot add new key
    """

    def __init__(self, dictionary):
        self._dictionary = dictionary

    def __setitem__(self, key, item):
        if key not in self._dictionary:
            raise KeyError("The key {} is not defined.".format(key))
        self._dictionary[key] = item

    def __getitem__(self, key):
        return self._dictionary[key]


class SignalDict(dict):
    """
    Default dict of signal
    """

    def __missing__(self, sym):
        res = {'header_msg': '%s %s' % (sym, ba.fr.get_chi_name(sym)),
               'signal_msg': []}
        res = self[sym] = defaultdict(lambda: False, res)
        return res


# =============================================================================
# ------ Memory Dict ------
# =============================================================================
class MemoryDict(dict):
    """
    Making a default dict for stock and futures
    If key not exists, it will init new instances
    
    For efficiency, set the dayback to shorten period, like 60
    """
    recalc = True
    dayback = None

    def __missing__(self, sym):
        s = None
        if sym.isdigit():
            s = Stock(sym)
            s.dump(recalc= MemoryDict.recalc, dayback= MemoryDict.dayback)
        else:
            s = Commodities(sym)
            s.dump(recalc= MemoryDict.recalc, dayback= MemoryDict.dayback)
        res = self[sym] = s
        return res


# ------ oo handler ------
class DataHandler(object, metaclass=ABCMeta):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    # abcstractmethod is letting the python know this function will be implemented by subclasses
    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        To retrieve bars from a stored place
        """
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        To retrieve bars from a stored place
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low, 
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class AptiCSVDataHandler(DataHandler):
    """
    AptiCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, symbol_list, csv_dir, start_date):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events      = events
        self.csv_dir     = csv_dir
        self.symbol_list = symbol_list
        self.start_date  = start_date

        self.symbol_data        = {}
        self.last_bar_dic       = {}
        self.continue_backtest  = True       
        self.bar_index          = 0

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, '%s.csv' % s),
                header=0, index_col=0, parse_dates=True,
                names=[
                    'Date', 'Open', 'High', 
                    'Low', 'Close', 'Vol', 'adj_close'
                ]
            ).sort_index()

            # trim the date by self.start_date
            self.symbol_data[s] = self.symbol_data[s][self.start_date:]
            
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.last_bar_dic[s] = []
        
        # Reindex the dataframes
        for s in self.symbol_list:
            #print(self.symbol_data[s][start_date:])
            self.symbol_data[s] = self.symbol_data[s].\
                reindex(index=comb_index, method='pad').iterrows()
            

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield b # 带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator，调用 fab(5) 不会执行 fab 函数，而是返回一个 iterable 对象

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.last_bar_dic[s].append(bar)
        self.events.put(MarketEvent())


class SQLiteDataHandler(DataHandler):
    """
        Reading Database
        """
    def __init__(self, events, symbol_list, db_name, start_date):

        self.events      = events
        self.db_path     = "C:/csv/" + db_name + ".db"
        self.symbol_list = symbol_list
        self.start_date  = start_date

        self.symbol_data        = {}
        self.last_bar_dic = {}
        self.continue_backtest  = True
        self.bar_index          = 0

        self._open_db()

    def _open_db(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.sql.read_sql("""
            SELECT Date, Open, High, Low, Close,Vol FROM ex_eod
            WHERE sym = '{0}'
            """.format(s.lstrip('0')), sqlite3.connect(self.db_path),index_col='Date',parse_dates='Date'
            ).sort_index()

            # trim the date by self.start_date
            self.symbol_data[s] = self.symbol_data[s][self.start_date:]
            
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.last_bar_dic[s] = []
            
        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].\
                reindex(index=comb_index, method='pad').iterrows()

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        return a generator, and the bar data
        """
        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        #print "get_latest_bar"
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        #print("get_latest_bars")
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        #print("get_latest_bar_value")
        try:
            bars_list = self.last_bar_dic[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        #print "get_latest_bars_values"
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Call this every heartbeat
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
                #print s,bar
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.last_bar_dic[s].append(bar)
        self.events.put(MarketEvent())


def task(s):
    """
    For concurrent
    """
    memory_db = MemoryDict()
    obj = memory_db[s]
    return obj


class PostgresDataHandler(DataHandler):
    """
    Postgresql Handler
    """

    def __init__(self, events, *args, **kwargs):

        self.events = events
        
        if args:
            self.logger = args[0]

        # self.symbol_data        = {}
        # self.latest_symbol_data = {}
        # self.continue_backtest = True
        # self.bar_index = 0

        self.params = kwargs

        # self._reading_data_from_db()

        # New Properties
        self.last_bar_dic = {}
        self.memory_db = MemoryDict()  # <- this cannot be directly using by Concurrent
        self.ticker_loaded = []  # <- This will loaded with list after init_memory_db

        # For backtest
        self.insample_end_day = None
        self.outsample_end_day = None
        self.memory_insample_db = {}
        self.memory_outsample_db = {}

    #    def _reading_data_from_db(self):
    #
    #        comb_index = None
    #        for s in self.symbol_list:
    #
    #            # Check if the symbol is digits
    #            if s.isdigit():
    #                if self.params['period'] == 'D':
    #                    self.symbol_data[s] = ba.tc.get_bar_ex_eod_db_recalc(s)
    #                elif self.params['period'] == '1min':
    #                    self.symbol_data[s] = ba.rtq.get_1min_sql(s, dayback= self.params['dayback'], startday= self.params['startday'])
    #            elif s.isalpha():
    #                if self.params['period'] == 'D':
    #                    self.symbol_data[s] = ba.f.get_futures_daybar_sql(s, dayback= self.params['dayback'], startday= self.params['startday'])
    #                elif self.params['period'] == '1min':
    #                    self.symbol_data[s] = ba.f.get_futures_sql(s, dayback= self.params['dayback'], startday= self.params['startday'])
    #
    #            # Combine the index to pad forward values
    #            if comb_index is None:
    #                comb_index = self.symbol_data[s].index
    #            else:
    #                comb_index.union(self.symbol_data[s].index)
    #
    #            # Set the latest symbol_data to None
    #            self.latest_symbol_data[s] = []
    #
    #        # Reindex the dataframes
    #        for s in self.symbol_list:
    #            self.symbol_data[s] = self.symbol_data[s].\
    #                reindex(index=comb_index, method='pad').iterrows()

    def return_db_obj(self, sym):
        return self.memory_db[sym]

    def trim_data(self, insample_end_day, outsample_end_day):
        """
        by day:
            bp.trim_data('20190116')
        by period:
            bp.trim_data('20190111','20190116')
        """
        from copy import copy
        from BATrader.market.MarketReader import determine_product_type

        # This steps is important: To tell data_handler the backtest period
        self.insample_end_day = insample_end_day
        self.outsample_end_day = outsample_end_day

        for sym in list(self.memory_db.keys()):  # the inited symlist
            product = determine_product_type(sym)
            self.memory_insample_db[sym] = ''  # later filled by object
            self.memory_outsample_db[sym] = ''  # later filled by object

            if product == 'Stocks':
                i = copy(self.memory_db[sym])  # This is the object
                o = copy(self.memory_db[sym])
                for data_field in Stock.meta:
                    ins = getattr(i, data_field)  # dataframe
                    outs = getattr(o, data_field)  # dataframe
                    setattr(i, data_field, ins[:insample_end_day])
                    setattr(o, data_field, outs[ba.dk.get_next_tradeday(insample_end_day): outsample_end_day])

                self.memory_insample_db[sym] = self.memory_db[sym] = i
                self.memory_outsample_db[sym] = o

            elif product == 'Futures':
                i = copy(self.memory_db[sym])  # This is the object
                o = copy(self.memory_db[sym])
                for data_field in Commodities.meta:
                    ins = getattr(i, data_field)  # dataframe
                    outs = getattr(o, data_field)  # dataframe
                    setattr(i, data_field, ins[:insample_end_day])
                    setattr(o, data_field, outs[ba.dk.get_next_tradeday(insample_end_day): outsample_end_day])

                self.memory_insample_db[sym]  = self.memory_db[sym] = i
                self.memory_outsample_db[sym] = o

    def init_memory_db(self, symlist: list, method: str = 'Thread') -> bool:
        """
        The main function to init the memory db.
        There are serveral method to fill up the self.memory_db

        memory_db
            An default dict which wrap two products into a handy in-memory database
            Also encapsulate the format and data returned

        Normal:
            Using a normal for loop. The slowest
        Pool
        Thread

        Like : pool or thread
        Args:
            symlist:
            method ():

        Returns:
            After init, return True
        """

        def init_memory_db_threading(_symlist):
            """
            memory_db
            An default dict which wrap two products into a handy in-memory database
            Also encapsulate the format and data returned
            """
            stock_queue = queue.Queue()

            thread_no = 3

            def worker():
                while not stock_queue.empty():
                    sym = stock_queue.get()
                    self.memory_db[sym]
                    if sym not in self.ticker_loaded:
                        self.ticker_loaded.append(sym)
                    stock_queue.task_done()

            for sym in symlist:
                stock_queue.put(sym)

            for i in range(thread_no):
                t = Thread(target=worker)
                t.daemon = True
                t.start()
                t.join()

        def init_memory_db_thread(_symlist):

            # There is problems with self, damn
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(task, s) for s in _symlist]
                for f in as_completed(futures):
                    if f.done():
                        obj = f.result()
                        self.logger.info('Init %s success' % obj.sym)
                        self.memory_db[obj.sym] = obj

        def init_memory_db_pool(_symlist):
            """
            There is problems with self, damn.
            The task need to ouside the self class.
            Args:
                symlist ():

            Returns:

            """
            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(task, s) for s in symlist]
                for f in as_completed(futures):
                    if f.done():
                        obj = f.result()
                        self.logger.info('Init %s success' % obj.sym)
                        self.memory_db[obj.sym] = obj

        if method == 'Pool':
            self.logger.info('Init memory db, mode : Pool')
            init_memory_db_pool(symlist)
        elif method == 'Thread':
            self.logger.info('Init memory db, mode : Thread')
            init_memory_db_thread(symlist)
        elif method == 'Threading':
            self.logger.info('Init memory db, mode : Threading')
            init_memory_db_threading(symlist)
        else:
            self.logger.info('Init memory db, mode : Normal')
            for sym in symlist:
                self.memory_db[sym]
        self.ticker_loaded = symlist
        return True

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bar(self, sym):
        """
        Returning a dict
        {'Chg': -0.05, 'Vol': 146454473.0, 'High': 6.5, 'Date': '20181114', 'Amount': 936787697.59, 'Low': 6.35, 'Time': '15:54:02', 'Chgp': -0.78, 'Close': 6.39, 'Open': 6.5}
        """
        return self.last_bar_dic[sym]

    def get_latest_bars(self, sym, N=1, frame='daybar'):
        try:
            return self.memory_db[sym].daybar
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_intraday_bars(self, sym):
        """
        Return the object's data1min dataframe
        """
        return self.memory_db[sym].data1min

    def get_latest_bar_datetime(self, sym):
        dic = self.last_bar_dic[sym]
        if 'Date' in dic:
            return dic['Date'] + ' ' + dic['Time']
        else:
            return dic['Datetime']

    def get_latest_bar_value(self, sym, val_type):
        try:
            bars_list = self.last_bar_dic[sym]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[val_type]
            # return getattr(bars_list[-1][1], val_type) #<- not work for futures

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        # print "get_latest_bars_values"
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self, data):
        """
        Action got a tick from pricer
        and let data_handler update 
        
        stocks  : 9:31 - 16:00
        futures : 9:15 - 16:29
        """

        product = data['product']
        sym = data['sym']
        if product == 'Stocks':

            self.logger.debug('update stocks bars of {}'.format(sym))
            s = self.memory_db[sym]
            s.update(data)

            # Backtesting, the tc data is empty
            #            if len(data['tc']) == 0:
            #                rtq_bar_dic = ba.algo.simulate_tc_dic(s.data1min)
            #            else:
            #                # latest bar
            #                rtq_bar_dic = data['tc']

            #            latest_bar = s.daybar.iloc[-1]
            #             = rtq_bar_dic

            self.last_bar_dic[sym] = s.data1min.reset_index().iloc[-1].to_dict()

            self.events.put(MarketEvent('stocks', sym, self.last_bar_dic[sym]))

        elif product == 'Futures':

            self.logger.debug('update futures bars of {}'.format(sym))
            f = self.memory_db[sym]
            f.update(data)

            #            if len(data['summary']) == 0:
            #                data['summary'].update(f._init_f())
            #
            #            # latest bar
            #            self.summary = data['summary']
            self.last_bar_dic[sym] = f.data1min.reset_index().iloc[-1].to_dict()

            self.events.put(MarketEvent('futures', sym, self.last_bar_dic[sym]))

    #    def refreshed(self):
    #        self.events.put(RefreshEvent())

    # ------ 數據 ------
    def get_market_turnover(self):
        """
        大市成交額
        """
        if not hasattr(self, 'market_turnover'):
            self.market_turnover = df = ba.fr.dbm.querySQL_postgresql("""
                                       SELECT "Date", "Vol" 
                                       FROM hsi
                                       """).set_index('Date')
            return df
        else:
            return self.market_turnover





# class TencentDataHandler(DataHandler):
#    
#    def __init(self, events, symbol_list):
#        
#        self.events = events
#        self.db_path = "C:/csv/eod.db"
#        self.symbol_list = symbol_list
#
#    def get_latest_bar(self, sym):
#        df = psql.read_sql("""SELECT Date,Open,High,Low,Close,Vol  
#                            FROM ex_eod WHERE sym = %s ORDER BY Date DESC LIMIT 1
#                            """ % sym, sqlite3.connect('C:/csv/eod.db'))
#        return df
#    
#    def get_latest_bars(self, sym, N=1):
#        df = psql.read_sql("""SELECT Date,Open,High,Low,Close,Vol  
#                            FROM ex_eod WHERE sym = %s
#                            """ % sym, sqlite3.connect('C:/csv/eod.db'))
#        return df.iloc[-N:]
#    
#    def get_latest_bar_datetime(self, sym):
#        pass
#    def get_latest_bar_value(self, sym, val_type):
#        pass
#    def get_latest_bars_values(self, sym, val_type, N=1):
#        pass
#    def update_bars(self):
#        self.events.put(MarketEvent())


# =============================================================================
# ------ backtrader ------
# =============================================================================
import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds


# daybar_df = ba.tc.get_bar_ex_eod_db_recalc('700')
# ba.algo.set_data(daybar_df)
# ba.algo.Signal_3_in_row()
# df        = ba.algo.data

## try rename the column
# col = df.columns.tolist()
#
# class PandasData_Signal(btfeeds.PandasData):
#    lines  = tuple(col)
#    params = tuple([tuple((x, -1)) for x in lines])
#    datafields = btfeeds.PandasData.datafields + (list(col))
#
#
# data = PandasData_Signal(dataname= df,
#                            volume=4,
#                            fromdate=datetime.datetime(2019, 1, 1),
#                            #todate=datetime.datetime(2015, 1, 1)
#                            )

class PandasData_PE(btfeeds.PandasData):
    lines = ('ma20', 'ma60', 'T3B')

    # openinterest in GenericCSVData has index 7 ... add 1
    # add the parameter to the parameters inherited from the base class
    params = (('ma20', -1),
              ('ma60', -1),
              ('T3B', -1))

    btfeeds.PandasData.datafields = btfeeds.PandasData.datafields + ['ma20', 'ma60', 'T3B']


class PandasData(bt.feed.DataBase):
    '''
    The ``dataname`` parameter inherited from ``feed.DataBase`` is the pandas
    DataFrame
    '''

    params = (
        # Possible values for datetime (must always be present)
        #  None : datetime is the "index" in the Pandas Dataframe
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('datetime', None),

        # Possible values below:
        #  None : column not present
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', -1),
        ('volume', -1),
    )


class Backtrader_DataHandler(object):

    def pandas_handler(self, df, startday=None, endday=None):
        # try rename the column
        col = df.columns.tolist()

        class PandasData_Signal(btfeeds.PandasData):
            lines = tuple(col)
            params = tuple([tuple((x, -1)) for x in lines])
            datafields = btfeeds.PandasData.datafields + (list(col))

        data = PandasData_Signal(dataname=df,
                                 volume=4,
                                 fromdate=startday,  # datetime.datetime(2019, 1, 1)
                                 todate=endday
                                 )

        return data

    def csv_handler(self, csv_path):
        datapath = "C:\\Users\\Branson\\Dropbox\\myProgramming\\Python_and_C\\backtrader-master\\datas\\orcl-1995-2014.txt"

        # Create a Data Feed
        data = bt.feeds.YahooFinanceCSVData(
            dataname=datapath,
            # Do not pass values before this date
            fromdate=datetime.datetime(2000, 1, 1),
            # Do not pass values after this date
            todate=datetime.datetime(2000, 12, 31),
            reverse=False)
        return data


if __name__ == '__main__':
    MemoryDict.recalc = False
    MemoryDict.dayback = 30
    memory_dict = MemoryDict()