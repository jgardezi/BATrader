# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 19:07:04 2019

@author: Branson
"""

from threading import Thread

import pprint
import queue as queue
import zmq
import argparse

from time import sleep
import talib as ta
import BATrader as ba

from BATrader.market.MarketReader import currentTime, currentTimeSecond, stopwatch, market_time_keeper

import pandas as pd
from BATrader.utils.toolbox import timeit

from BATrader.final.data_handler import PostgresDataHandler, MemoryDict
from BATrader.final.execution import TraderExecutionHandler, SpExecutionHandler
from BATrader.final.strategy import DemoMonitor, DemoFuturesMonitor
from BATrader.final.portfolio import BacktestPortfolio
from BATrader.final.action import MetaAction

def day_end_write_report(func):
    def wrapper(*args):
        func(*args)

    return wrapper


# =============================================================================
#   Symlist
# =============================================================================
def symlist_cleanlist():
    """
    Clean list function , with sorting
    Returns:
    """
    return ba.fr.get_cleanlist()


def symlist_self_pick_stocks() -> list:
    return ba.sm.tsci_symlist_self_pick_stock()


# =============================================================================
#   Classes
# =============================================================================
class Monitor(MetaAction):
    """
    The base class of Monitor

    A monitor is an instance that will loop certtain criteria and certain list,
    When condition matched, Monitor will notify the User (Me in the case so far)
    Montitor should have the ability to communicate with Trader and pass signals

    Defined:
        What is the target symlist ? is it dynamic or fixed

    Steps:
        First the monitor should init with data in database.
        Get the data update
        A monitor can have their own filter, screen out those not matched
        Run the test

    """
    def __init__(self, config_name, *args, **kwargs):

        # Init base class
        super(Monitor, self).__init__(config_name)
        self.params_dict = kwargs

        self.data_handler_cls      = args[0]
        self.strategy_cls          = args[1]
        self.portfolio_cls         = args[2]  # Currently we don't implement this
        self.execution_handler_cls = args[3]  # Currently we don't implement this
        self.symlist_func          = lambda: args[4] # lazy execute

        self.ticker_waiting_to_subscribe = self.symlist_func()

        self.tradeday = ba.dk.tradeday()
        self.pTradeday = ba.dk.tradeday(1)

        # holder of sleep period
        self.interval = 0

        # Monitor report after running
        self.write_to_signal_report = False

        # For Backtest
        self.backtest_day = None

        # Generate Trading instances
        self.logger.info("Creating DataHandler, Strategy, Portfolio and ExecutionHandler")
        self.data_handler = self.data_handler_cls(self.events, self.logger, **kwargs)
        self.strategy = self.strategy_cls(self.data_handler, self.events, self.logger, **kwargs)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.logger, self.tradeday,
                                            **kwargs)
        self.execution_handler = self.execution_handler_cls(self.events, self.portfolio, self.logger)       

    def trim_backtest_data(self, insample_end_day, outsample_end_day):
        """
        While we loaded all data,
        we need to trim the data and left only what we needed

        1. Trim the data bars in data handler
        2. pass them to pricer
        3. told pricer to begin backtest
        """
        backtest_day = (insample_end_day, outsample_end_day)
        self.backtest_day = backtest_day

        # uppack the tuple before send, or it became tuple of tuple
        self.logger.debug('Trimming the data %s %s' % (insample_end_day, outsample_end_day))
        self.data_handler.trim_data(insample_end_day, outsample_end_day)

        work_message = {'backtest_day': backtest_day}
        self.notify_pricer(work_message)

        # notify every outsample data to pricer by looping the db
        for sym in list(self.data_handler.memory_outsample_db.keys()):
            self.logger.debug('Passing Outsample Data : {}, {} bars'.format(sym, len(self.data_handler.memory_outsample_db[sym].data1min)))
            work_message = {'backtest_bars': self.data_handler.memory_outsample_db[sym],
                            'sym': sym}
            self.notify_pricer(work_message)

    def start_monitoring(self):
        """
        Start Montioring
        Returns:

        First the monitor should init with data in database.
        Get the data update
        A monitor can have their own filter, screen out those not matched
        Run the test

        """
        # First told pricer to stop if it's backtesting
        if self.current_config_name == 'backtest':
            # First stop the pricer if it's running
            work_message = dict(backtest_end='')
            self.notify_pricer(work_message)
        
        # Return true if success init, and clear the self.ticker_waiting_to_subscribe
        loaded = self.data_handler.init_memory_db(self.ticker_waiting_to_subscribe)
        if loaded:
            self.subscribe_ticker(self.ticker_waiting_to_subscribe)
            self.ticker_waiting_to_subscribe = []
            
        if self.current_config_name == 'backtest':
            
            self.logger.info('Running Monitor in backtest mode')

            # get the insample outsample day
            insample_day   = self.params_dict['inday']
            outsample_day  = self.params_dict['outday']
                
            self.trim_backtest_data(insample_day, outsample_day)
            
            # Not impletment this right now
            # self.portfolio.construct_portfolio(self.subscribed_ticker)
            
            # backtest timer
            if 'backtest_timer' in self.params_dict:
                work_message = {'backtest_timer': self.params_dict['backtest_timer']}
                self.notify_pricer(work_message)

            # Tell pricer to start backtest
            self.logger.info('Telling Pricer to start backtest')
            work_message = {'backtest_begin': True}
            self.notify_pricer(work_message)

        self.start_listening()


if __name__ == '__main__':
    
    monitor = Monitor('production', PostgresDataHandler, DemoMonitor, BacktestPortfolio, TraderExecutionHandler,
                      ['939'], # ba.sm.symlist('ipo')
                      init_method='Thread')#, inday='20190701', outday='20190702') #, backtest_timer='1000')
    #monitor.start_monitoring()
    t1 = Thread(target= monitor.start_monitoring)
    #t1.start()
    
    
    monitor = Monitor('production', PostgresDataHandler, DemoFuturesMonitor, BacktestPortfolio, TraderExecutionHandler,
                      ['HSIF'],
                      init_method='Thread')
    #monitor.start_monitoring()
    t2 = Thread(target= monitor.start_monitoring)
    #t2.start()


    monitor = Monitor('backtest', PostgresDataHandler, DemoFuturesMonitor, BacktestPortfolio, TraderExecutionHandler,
                      ['HSIF'],
                      init_method='Thread', inday=ba.dk.get_prev_tradeday(ba.dk.tradeday()), outday=ba.dk.tradeday())
    #monitor.start_monitoring()
    t3 = Thread(target= monitor.start_monitoring)
#    t3.start()


    monitor = Monitor('backtest', PostgresDataHandler, DemoMonitor, BacktestPortfolio, TraderExecutionHandler,
                      ba.sm.get_recent_ipo_symlist(),
                      init_method='Thread', inday=ba.dk.get_prev_tradeday(ba.dk.tradeday()), outday=ba.dk.tradeday())
    #monitor.start_monitoring()
    t4 = Thread(target= monitor.start_monitoring)
    
