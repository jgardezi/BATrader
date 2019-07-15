# -*- coding: utf-8 -*-
"""
Created on Tue May 29 22:28:39 2018

@author: Branson
"""
import pprint
import queue as queue
from threading import Thread
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import zmq
import argparse

from time import sleep

import BATrader as ba
from BATrader.market.push import push_to_alchemy
from BATrader.fileReader import read_ini_config
from BATrader.market.MarketReader import currentTime, currentTimeSecond, stopwatch, market_time_keeper, is_stock
from BATrader.final.config_manager import ConfigManager
from BATrader.final.portfolio import BacktestPortfolio, LivePortfolio
from BATrader.final.strategy import DemoStrategy, DemoFuturesMonitor
from BATrader.final.data_handler import SignalDict
from BATrader.final.execution import SpExecutionHandler


from BATrader.logger import Logger

import pandas as pd


# =============================================================================
#   Component
# =============================================================================
from BATrader.final.data_handler import PostgresDataHandler, MemoryDict
from BATrader.final.execution import TraderExecutionHandler


# =============================================================================
# For concurrency -> init_memory_db_pool
# =============================================================================
def task(s):
    memory_db = MemoryDict()
    obj = memory_db[s]
    return obj


# =============================================================================
# Class
# =============================================================================


class MetaAction(object):
    """
    Meta Action is a metaclass that forms the behavior of Trading, backtesting and monitoring
    """
    def __init__(self, config_name):

        # logger ( [Date]_[Machine_Name]_[Name_of_Pricer_Class]_[config] )
        self.logger = Logger(
            logname=ba.fr.path_output + 'logger\\{0}_{1}_{2}_{3}.log'.format(ba.dk.get_today(), ba.fr.current_machine,
                                                                             self.__class__.__name__, config_name),
            logger=ba.fr.current_machine + self.__class__.__name__).getlog()

        # config
        self.current_config_name      = config_name
        self.config                   = ConfigManager(config_name)

        # list of subscribed symbol
        self.symlist = []  # Subscribed stock
        self.flist   = []  # Subscribed futures

        self.signal_dic = SignalDict()  # monitor signal
        self.dic = {}

        # subcribed ticker : symlist + flist
        self.subscribed_ticker = []

        # Ticker list waiting to be subcribed
        self.ticker_waiting_to_subscribe = []

        # Event queue
        self.events = queue.Queue()

        # Socket
        self.price_receiver = None

        # stats
        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1

    def connect_price_server(self):
        """
        Connect to price server
        
        Send we are using recv_pyobj, zeromq natively didn't know about the data.
        It's just pickling the data and transfer.
        so no filtering of messages can be dones
        """
        if not self.price_receiver:
            self.logger.debug("Created socket with : Pricer")
            context = zmq.Context()
            self.price_receiver = context.socket(zmq.SUB)
            self.price_receiver.connect(self.config.address_pub)
            #self.price_receiver.connect(self.config.address_stock)
            #self.price_receiver.connect(self.config.address_futures)
            self.price_receiver.setsockopt(zmq.SUBSCRIBE, b"")  # it means subscribe all

    def disconnect_price_server(self):
        """
        Disconnect the price server
        """
        if self.price_receiver:
            self.logger.debug('Disconnect Price Server')
            self.price_receiver.disconnect(self.config.address_pub)
#            self.price_receiver.disconnect(self.config.address_stock)
#            self.price_receiver.disconnect(self.config.address_futures)
            self.price_receiver = None

    def addticker(self, ticker: str):
        """
        Pass in str
        """
        self.ticker_waiting_to_subscribe.append(ticker)

    def notify_pricer(self, work_message):
        """
        Pricer listen work_message with a Pull
        """
        context = zmq.Context()
        zmq_socket = context.socket(zmq.PUSH)
        zmq_socket.bind(self.config.address_ticker)
        zmq_socket.send_pyobj(work_message)
        sleep(0.1)
        zmq_socket.unbind(self.config.address_ticker)

    def subscribe_ticker(self, ticker: list):
        """
        Subscribe the ticker
        Args:
            ticker:
        """
        self.logger.info('Subcribing ticker to pricer')
        # self.data_handler.init_memory_db(ticker)
        work_message = {'subscribe': ticker}
        self.notify_pricer(work_message)
        self.subscribed_ticker += ticker
        
        for sym in ticker:
            if is_stock(sym):
                self.symlist.append(sym)
            else:
                self.flist.append(sym)
        
        print('Done.')

    def unsubscribe_ticker(self, ticker: list):
        """
         Unsubscribe the ticker
        """
        self.logger.info('Unsubcribing')
        work_message = {'unsubscribe': ticker}
        self.notify_pricer(work_message)
        self.subscribed_ticker = list(set(self.subscribed_ticker) - set(ticker))
        
        for sym in ticker:
            if is_stock(sym):
                self.symlist -= [sym]
            else:
                self.flist -= [sym]
        
        print('Done.')

    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()
        
        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        
        print("Creating equity curve...")
        print(self.portfolio.equity_curve.tail(10))
        pprint.pprint(stats)

        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)

    def start_listening(self):
        """
        Start Listening is normal price initiate event,
        """
        # Connect the pricer
        self.connect_price_server()

        self.logger.info('Listening...')
        self.logger.info('Start trading... Good luck today :)')

        keep_running = True
        while keep_running:

            # Subscribe ticker here
            if self.ticker_waiting_to_subscribe:
                self.subscribe_ticker(self.ticker_waiting_to_subscribe)
                self.ticker_waiting_to_subscribe = []
                
            data = self.price_receiver.recv_pyobj()
            #print(data)

            if data['msg_type'] == 'data':

                # Check is it our subscribed stock
                if data['sym'] not in self.subscribed_ticker:
                    return
                
                self.logger.debug('Got data of: {}'.format(data['sym']))

                self.data_handler.update_bars(data)
                
                # May be we can put some hack in here, so that we can just receive
                # the price

                # Second loop
                while not self.events.empty():
    
                    event = self.events.get()
    
                    if event.type == 'MARKET':
                        # self.logger.info("Got an Market Event, calcualte signal now")
                        self.strategy.calculate_signals(event)
                        # self.logger.info("Update portfolio time index now")
                        
                        if 'Monitor' not in self.strategy.strategy_id:
                            self.portfolio.update_timeindex(event)

                    elif event.type == 'MONITOR':
                        self.logger.info(event.msg_list)
                        push_to_alchemy(event.msg_for_send)

                        for signal_name in event.signal_name_list:
                            # Append the monitor signal msg to self.signal_dic
                            if not self.signal_dic[event.sym][signal_name]:
                                self.signal_dic[event.sym]['signal_msg'] += event.msg_list
                                self.signal_dic[event.sym][signal_name] = True

                    elif event.type == 'SIGNAL':
                        # self.logger.info("Got an Signal Event !!! now pass to portfolio to gen order")
                        self.signals += 1
                        self.portfolio.update_signal(event)
    
                    elif event.type == 'ORDER':
                        # self.logger.info("Got an Order Event, pass to execution handler to execute")
                        self.orders += 1
                        self.execution_handler.execute_order(event)
                        # event.print_order()
    
                    elif event.type == 'FILL':
                        # self.logger.info("Got an Fill Event")
                        self.fills += 1
                        self.portfolio.update_fill(event)
                        self.logger.info("Trade completed, return to action")
                        self.portfolio.print_portfolio()
    
            elif data['msg_type'] == 'msg':
                
                if data['msg_body'] == 'stop_running':
                    self.logger.info('Price server told me to shut down, I am going.')
                    #self.disconnect_price_server()
                    #self.logger.info('Disconnected price server.')
                    keep_running = False
                    
                    if 'Monitor' not in self.strategy.strategy_id:
                        self._output_performance()

                elif data['msg_body'] == 'refresh_done':
                    # actually we don't need a refrehed event
                    # after refreshed, just regen the target symlist
                    self.logger.debug("Pricer Refreshed, let me regen the target")
                    self.logger.debug("-----------------------------------------")
                    self.logger.debug("")
                    for sym in self.symlist_func():
                        if sym not in self.subscribed_ticker:
                            self.ticker_waiting_to_subscribe.append(sym)
                            
                elif data['msg_body'] == 'pricer_push':
                    # pricer wanna straightly sending push
                    # let Monitor push only
                    if 'Monitor' in self.strategy.strategy_id:
                        push_to_alchemy(data['push_msg'])
                        
                elif data['msg_body'] == 'observe_this':
                    # let Monitor observe only
                    if 'Monitor' in self.strategy.strategy_id:
                        self.ticker_waiting_to_subscribe.append(data['observe_list'])


class Trader(MetaAction):
    """
    Enscapsulates the settings and components for carrying out
    a real live trading.

    The Final
        v3.0 2015-10
        V3.4 2017-10
        V3.5 2018-03
        V4.0 2018-04 Change TheFinal3 to Trader, migrating the new Trader class
        v5.0 2018-05 Changed to three-pipeline model
        v5.1 2018-08 adding some more
        v5.2 2018-08 adding config
        v5.3 2019-01 its too hard to implement this, will combined Trader class to Action

    start()
    ------
    the entry point, it repeatly runs gen_signal with a timer

    gen_signal(self, symlist, pricer, cond_filter, tester)

    Usage:
        trader.gen_signal(trader._symlist_testlist(),\
                         trader._pricer_local_test,\
                         trader._filter_production,\
                         trader.algo_tester)

    ::: BUGS :::
        20180202 The trader won't sleep when lunch! (20180403 Fixed)

    20190510 The Trade action becomes "Trader", trying to stick to the OObacktest

        Parameters:
            data_handler_cls,
            strategy_cls,
            portfolio_cls,
            execution_handler_cls,
            symbol_list_func,

    """

    def __init__(self, config_name, *args, **kwargs):

        # Init base class, it's either production or debug
        super(Trader, self).__init__(config_name)
        self.params_dict = kwargs

        self.data_handler_cls      = args[0]
        self.strategy_cls          = args[1]
        self.portfolio_cls         = args[2]
        self.execution_handler_cls = args[3]
        self.symlist_func          = lambda: args[4] # lazy execute
        
        self.ticker_waiting_to_subscribe = self.symlist_func()

        # find today and pday
        self.tradeday = ba.dk.tradeday()
        self.pTradeday = ba.dk.tradeday(1)
        
        # Pass in or get it?
        #self.initial_capital       = self.params_dict['initial_capital']

        # Generate Trading instances
        self.logger.info("Creating DataHandler, Strategy, Portfolio and ExecutionHandler : Trader")
        self.data_handler = self.data_handler_cls(self.events, self.logger, 
                                                  **kwargs)
        self.strategy = self.strategy_cls(self.data_handler, self.events, self.logger, 
                                          **kwargs)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.logger, self.tradeday,
                                            **kwargs)
        self.execution_handler = self.execution_handler_cls(self.events, self.portfolio, self.logger,
                                                            **kwargs)

    def start_trading(self):

        # Return true if success init, and clear the self.ticker_waiting_to_subscribe
        loaded = self.data_handler.init_memory_db(self.ticker_waiting_to_subscribe)
        if loaded:
            self.subscribe_ticker(self.ticker_waiting_to_subscribe)
            self.ticker_waiting_to_subscribe = []

        # First construct the portfolio
        self.portfolio.construct_portfolio(self.subscribed_ticker)

        self.start_listening()


class Backtester(MetaAction):
    """
    Enscapsulates the settings and components for carrying out
    an event-driven backtest.

    symbol_list, initial_capital, data_handler, execution_handler, portfolio, strategy,

    backtest_timer= '0940'
        Parameters:
            csv_dir - The hard root to the CSV data directory.
            symbol_list - The list of symbol strings.
            intial_capital - The starting capital for the portfolio.
            heartbeat - Backtest "heartbeat" in seconds
            start_date - The start datetime of the strategy.
            data_handler - (Class) Handles the market data feed.
            execution_handler - (Class) Handles the orders/fills for trades.
            portfolio - (Class) Keeps track of portfolio current and prior positions.
            strategy - (Class) Generates signals based on market data.

    20190709
        Parameters:
            data_handler_cls,
            strategy_cls,
            portfolio_cls,
            execution_handler_cls,
            symbol_list_func,
            
        Must pass in kwargs:
            initial_capital
            inday
            outday
    
    """
    def __init__(self, config_name, *args, **kwargs):

        # Init base class
        super(Backtester, self).__init__(config_name)
        self.params_dict = kwargs

        self.data_handler_cls      = args[0]
        self.strategy_cls          = args[1]
        self.portfolio_cls         = args[2]
        self.execution_handler_cls = args[3]
        self.symlist_func          = lambda: args[4] # lazy execute
        
        self.ticker_waiting_to_subscribe = self.symlist_func()

        # Backtest params
        self.initial_capital       = self.params_dict['initial_capital']
        self.insample_day   = self.params_dict['inday']
        self.outsample_day  = self.params_dict['outday']

        # For Backtest
        self.backtest_day = None

        # Generate Trading instances
        self.logger.info("Creating DataHandler, Strategy, Portfolio and ExecutionHandler : Backtest")
        self.data_handler = self.data_handler_cls(self.events, self.logger, **kwargs)
        self.strategy = self.strategy_cls(self.data_handler, self.events, self.logger, **kwargs)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.logger, self.insample_day,
                                            **kwargs)
        self.execution_handler = self.execution_handler_cls(self.events, self.portfolio, self.logger)      

    def start_backtesting(self):
        """
        Simulates the backtest and outputs portfolio performance.
        It use zeromq
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
        
        
    def start_internal_backtesting(self):
        """
        Backtesting without zeromq. All backtest runs internally
        """
        """
        Executes the backtest.
        """
        i = 0
        while True:
            i += 1
            print(i)
            # Update the market bars
            if self.data_handler.continue_backtest == True:
                self.data_handler.update_bars()
            else:
                break

            # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_timeindex(event)

                        elif event.type == 'SIGNAL':
                            self.signals += 1                            
                            self.portfolio.update_signal(event)

                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)

                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)

            sleep(self.heartbeat)


        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()
        
        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        
        print("Creating equity curve...")
        print(self.portfolio.equity_curve.tail(10))
        pprint.pprint(stats)

        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)
        
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
            self.logger.debug('Passing Outsample Data : %s' % sym)
            work_message = {'backtest_bars': self.data_handler.memory_outsample_db[sym],
                            'sym': sym}
            self.notify_pricer(work_message)




if __name__ == '__main__':

    print('Running in spyder')
    parser = argparse.ArgumentParser(description='Action Director')  # 動作指導
    parser.add_argument("model", default='d', nargs='?')  # read ini config
    #parser.add_argument("strategy", nargs="?")
    args = parser.parse_args()

    dic = {'p': 'production',
           'd': 'debug',
           'b': 'backtest'}

    # ============================================================================
    #   Production and debug
    # =============================================================================
    if args.model != 'b':
        
        mode = 'sp'
        
        if mode == 'sp':
            trader = Trader(dic[args.model], PostgresDataHandler, DemoStrategy, LivePortfolio, SpExecutionHandler,
                           ['HSIF'])
            trader.start_trading()
        else:
            trader = Trader(dic[args.model], PostgresDataHandler, DemoStrategy, BacktestPortfolio, TraderExecutionHandler,
                           ['HSIF'])
            trader.start_trading()

    # =============================================================================
    #   Backtest
    # =============================================================================
    elif args.model == 'b':
        
        backtester = Backtester('backtest', PostgresDataHandler, DemoFuturesMonitor, BacktestPortfolio, TraderExecutionHandler,
                       ['HSIF'],
                       initial_capital= 100000, inday='20190610', outday='20190611')
        #backtester.start_backtesting()
        
    # =============================================================================
    #   Reply
    #   replay is a kind of backtest, which is shorthanded for replaying 
    #   current trade day market siutation
    # =============================================================================
    elif args.model == 'replay':

        b = Backtester('backtest',  PostgresDataHandler, DemoStrategy, BacktestPortfolio, TraderExecutionHandler,
                       ['HSIF'],
                       inday=ba.dk.tradeday(1), outday=ba.dk.tradeday())
        b.start_backtesting()

