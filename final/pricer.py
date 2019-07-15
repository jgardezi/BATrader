# -*- coding: utf-8 -*-
"""
Created on Tue May 29 19:34:42 2018

@author: Branson
"""
import argparse
from threading import Thread
import zmq

import BATrader as ba
import pandas as pd
from BATrader.market.MarketReader import market_time_keeper, market_time_hk_futures, currentTime, currentTimeSecond, is_stock
from BATrader.logger import Logger

from time import sleep, strftime, time
from copy import copy
import traceback

from BATrader.final.data_handler import quote_dic, msg_dic
from BATrader.final.config_manager import ConfigManager

# from abc import ABC, abstractmethod

import redis
from datetime import datetime


class MetaServer(object):

    def __init__(self, config_name='debug'):
        # logger ( [Date]_[Machine_Name]_[Name_of_Pricer_Class]_[config] )
        self.logger = Logger(
            logname= ba.fr.path_output + 'logger\\{0}_{1}_{2}_{3}.log'.format(ba.dk.get_today(), ba.fr.current_machine,
                                                                             self.__class__.__name__, config_name),
            logger= ba.fr.current_machine + self.__class__.__name__).getlog()

        # config
        self.current_config_name = config_name
        self.config = ConfigManager(config_name)

        # list of subscribed symbol
        self.symlist = []
        self.flist = []

        # clear the redis server
        # MetaServer.clear_all_table_redis()

        # Socket
        # self.pricer_stock_socket   = None
        # self.pricer_futures_socket = None
        self.pricer_pub_socket = None

        # Backtest Server
        self.backtest_bars = {}  # current support 1 min bar
        self.backtest_day = ''  # get from the ticket listener
        self.backtest_begin = False
        self.continue_backtest = False
        self.backtest_range = ''
        self.backtest_timer = None
        
        self.current_backtest_day = ''  # for iteration
        self.current_backtest_time = ''  # for iteration also, type datetime.time
    
        self.prev_stock_backtest_time = ''
        self.prev_futures_backtest_time = ''

    def generate_datetime_range_and_backtest_bars(self) -> list:
        """
        This is to generate the datetime range and backtest bars

        Returns:
            self.backtest_range
        """
        if self.backtest_day:
            self.logger.debug('Generate backtest datetime range')
            period_tuple = self.backtest_day

            # Calculate the day range
            rng = []
            for day in ba.dk.date_ranger(ba.dk.get_next_calendar_day(period_tuple[0]),
                                         ba.dk.get_next_calendar_day(period_tuple[1])):
                for t in pd.date_range("%s 9:14" % day, "%s 16:30" % day, freq='min'):
                    if t not in pd.date_range("%s 12:01" % day, "%s 12:59" % day, freq='min'):
                        rng.append(t)

            self.backtest_range = rng

            return rng

    def listen_for_work_message_request(self):
        """
        The Price Server listen for the ticker request from action.
        Also backtest behaviors are handled

        Listen  work_message in PULL, infinisty:
            work_message is a dict like this:
                work_message = { 'backtest_end' : ''}

        """

        self.logger.debug("I am listening, told me what you need.")
        context = zmq.Context()
        # recieve work  
        consumer_receiver = context.socket(zmq.PULL)
        consumer_receiver.connect(self.config.address_ticker)

        while True:
            try:
                work_message = consumer_receiver.recv_pyobj()
                if 'subscribe' in work_message:
                    self.logger.info('Got a ticker subscribe request : %s' % work_message['subscribe'])
                    for item in work_message['subscribe']:
                        if is_stock(item):
                            if item in self.symlist:
                                self.logger.info('Already subscribed, skip.')
                            else:
                                MetaServer.subscribe("SubscribedStockTicker", item)
                                self.symlist.append(item)
                                self.logger.info('Subscribed %s.' % item)
                        else:
                            if item in self.flist:
                                self.logger.info('Already subscribed, skip.')
                            else:
                                MetaServer.subscribe("SubscribedFuturesTicker", item)
                                self.flist.append(item)
                                self.logger.info('Subscribed %s.' % item)
                elif 'unsubscribe' in work_message:
                    un_list = work_message['unsubscribe']
                    self.logger.info('Got an unsubscribe')
                    self.symlist = list(set(self.symlist) - set(un_list))
                    self.flist = list(set(self.flist) - set(un_list))
                # Backtesting 
                elif 'backtest_day' in work_message:
                    self.logger.info('Got backtest day')
                    self.backtest_day = work_message['backtest_day']  # tuple
                elif 'backtest_bars' in work_message:
                    sym = work_message['sym']
                    self.backtest_bars[sym] = work_message['backtest_bars']
                    self.logger.debug('Got {} bars of backtest bars: {}'.format(len(work_message['backtest_bars'].data1min), sym))
#                    # shift the futures bar
#                    if sym == 'HSIF' or sym == 'HHIF' or sym == 'MHIF' or sym == 'MHCF':
#                        self.backtest_bars[sym].data1min = self.backtest_bars[sym].data1min.shift(1, '1Min')

                elif 'backtest_begin' in work_message:
                    self.logger.info('Action told me to begin backtest')
                    self.generate_datetime_range_and_backtest_bars()
                    thread_b = Thread(target=self.start_backtest)
                    thread_b.start()

                elif 'backtest_end' in work_message:
                    self.logger.info('Action told me to stop backtest')
                    self.symlist = []
                    self.flist   = []
                    self.backtest_bars = {}
                    self.backtest_begin = False
                    self.continue_backtest = False
                    self.backtest_timer = None

                    try:
                        if thread_b.is_alive():
                            thread_b.join()
                    except NameError:
                        pass

                elif 'backtest_timer' in work_message:
                    
                    self.backtest_timer = datetime.strptime(work_message['backtest_timer'], "%H%M").time()

            except Exception as e:
                self.logger.info('Exception in listen:')
                self.logger.info(str(e))

    @staticmethod
    def clear_all_table_redis():
        conn = redis.Redis()
        conn.flushdb()

    @staticmethod
    def clear_subscribed(set_name: str):
        conn = redis.Redis()
        conn.delete(set_name)

    @staticmethod
    def subscribe(set_name: str, sym: str):
        conn = redis.Redis()
        conn.sadd(set_name, sym)
        
    def disconnect_self_socket(self):
        """
        Unbind both stock and futures channel
        """
        if self.pricer_pub_socket:
            address = self.pricer_pub_socket.getsockopt_string(zmq.LAST_ENDPOINT)
            self.pricer_pub_socket.unbind(address)
            self.pricer_pub_socket = None
        # self.disconnect_stock_socket()
        # self.disconnect_futures_socket()

    def terminate_action(self):
        """
        Action got the stop_running message.
        They will disconnect the price server, and kee
        Returns:

        """
        msg = copy(msg_dic)
        msg['msg_body'] = 'stop_running'

        self.pricer_pub_socket.send_pyobj(msg)
        # self.pricer_stock_socket.send_pyobj(msg)
        # self.pricer_futures_socket.send_pyobj(msg)
        
    def create_socket(self):
        """
        Create socket for stocks channel
        """
        if not self.pricer_pub_socket:
            context = zmq.Context()
            self.logger.info('Pricer Socket created : {}'.format(self.current_config_name))
            self.pricer_pub_socket = context.socket(zmq.PUB)
            self.pricer_pub_socket.bind(self.config.address_pub)

class StockPricer(MetaServer):

#    def create_stock_socket(self):
#        """
#        Create socket for stocks channel
#        """
#        if not self.pricer_stock_socket:
#            context = zmq.Context()
#            self.logger.info('Socket created : Stocks')
#            self.pricer_stock_socket = context.socket(zmq.PUB)
#            self.pricer_stock_socket.bind(self.config.address_stock)
#
#    def disconnect_stock_socket(self):
#        """
#        Unbind socket for stocks channel
#        """
#        if self.pricer_stock_socket:
#            address = self.pricer_stock_socket.getsockopt_string(zmq.LAST_ENDPOINT)
#            self.pricer_stock_socket.unbind(address)
#            self.pricer_stock_socket = None

    def get_stock_quote(self, sym):
        """
        Define the stock pickle format passed
        """
        msg = copy(quote_dic)  # if not made a copy, it will massing the data
        msg['product'] = 'Stocks'
        msg['sym'] = sym
        # msg['tc']   = ba.tc.tc_rtq(sym, mode='full', return_dic=True)
        msg['1min'] = ba.rtq.get_aa_1min(sym, get_one_day_only=True)

        return msg

    def refresh_and_push_stock(self):
        """
        Production :
            1. Some meta task can run here

        Refresh by calling get_stock_quote
            1. loop the symlist and pass the qutoe
            2. Send the meta message dict
        """
        if self.symlist:
            self.logger.debug('Refreshing and push stock price')
            for sym in self.symlist:
                quote_msg = self.get_stock_quote(sym)
                self.pricer_pub_socket.send_pyobj(quote_msg)

            # send a refreshed message
            msg = copy(msg_dic)
            msg['msg_body'] = 'refresh_done'
            self.pricer_pub_socket.send_pyobj(msg)
            
    def tell_action_to_push(self, msg_body):
        msg = copy(msg_dic)
        msg['msg_body'] = 'pricer_push'
        msg['push_msg'] = msg_body
        self.pricer_pub_socket.send_pyobj(msg)

    def start_stock_pricer(self):

        self.logger.info('Stock production started.')

        # Waiting 00
        while True:
            currentSecond = strftime('%S')
            if currentSecond== '00':
                break
        self.logger.info('SP: Time in 00, start now')

        # Start
        next_time = time() + self.config.rapid_market_interval_s

        sleep_interval = 1

        # Event Loop
        keep_running = True
        while keep_running:

            sleep(max(0, next_time - time()))
            tup = market_time_keeper()
            
            # Timer for task
            if currentTimeSecond() == '09:20:30':
                self.logger.warning("Let's look for HKEx news")
                try:
                    df = ba.hkex.hkexnews_recent()
                except:
                    self.logger.warning("Fail to check hkex")

            try:
                if tup[0] == 'morning' or tup[0] == 'afternoon':
                    # Rapid market 
                    if 929 < currentTime() <= 1030:
                        sleep_interval = self.config.rapid_market_interval_s
                        self.logger.debug('Stock production in Rapid Morning Session, refresh: %s' % sleep_interval)
                    elif 1300 <= currentTime() <= 1330:
                        sleep_interval = self.config.rapid_market_interval_s
                        self.logger.debug('Stock production in Rapid Afternoon Session, refresh: %s' % sleep_interval)
                    else:
                        sleep_interval = self.config.normal_market_interval_s
                        self.logger.debug('Stock production in Normal Session, refresh: %s' % sleep_interval)
    
                    self.refresh_and_push_stock()

    
                elif tup[0] == 'lunch':
                    sleep(5)  # if lunch hour, sleep and retry at interval 5s

    #            elif tup[0] == 'closed':
    #                self.terminate_action()
    
            except Exception:
                traceback.print_exc()
                
            delay = sleep_interval
            next_time += (time() - next_time) // delay * delay + delay

            # keep runnint
            keep_running = tup[1]


class FuturesPricer(MetaServer):

#    def create_futures_socket(self):
#        """
#        Create socket for futures channel
#        """
#        if not self.pricer_futures_socket:
#            context = zmq.Context()
#            self.logger.info('Socket created : futures')
#            self.pricer_futures_socket = context.socket(zmq.PUB)
#            self.pricer_futures_socket.bind(self.config.address_futures)
#
#    def disconnect_futures_socket(self):
#        """
#        Unbind socket for futures channel
#        """
#        if self.pricer_futures_socket:
#            address = self.pricer_futures_socket.getsockopt_string(zmq.LAST_ENDPOINT)
#            self.pricer_futures_socket.unbind(address)
#            self.pricer_futures_socket = None

    def get_futures_quote(self, sym):
        """
        Define the futures pickle format passed
        """
        #self.logger.info("FP: get_futures_quote")
        msg = copy(quote_dic)
        msg['product'] = 'Futures'
        msg['sym'] = sym
        # dic['summary'] = ba.f.fquote(product= sym)
        msg['1min'] = ba.f.get_aastocks_intraday_futures(product=sym) #, get_one_day_only=True)

        return msg

    def refresh_and_push_futures(self):
        """
        Refresh by calling get_futures_quote

        1. loop the symlist and pass the qutoe
        2. Send the meta message dict
        """
        if self.flist: 
            #self.logger.debug('Refreshing and push futures price')
            for sym in self.flist:
                quote_msg = self.get_futures_quote(sym)
                self.pricer_pub_socket.send_pyobj(quote_msg)

            # send a refreshed message
            msg = copy(msg_dic)
            msg['msg_body'] = 'refresh_done'
            self.pricer_pub_socket.send_pyobj(msg)


    # Futures
    def start_futures_pricer(self):
        """
        The Pricer will continue a loop defined with refresh intervel
        """
        self.logger.info('Futures production started.')

        # Waiting 00
        while True:
            currentSecond = strftime('%S')
            if currentSecond== '00':
                break
        self.logger.info('FP: Time in 00, start now')

        # Start
        next_time = time() + self.config.refresh_interval_f
        
        sleep_interval = 1
        
        # Event Loop
        keep_running = True
        while keep_running:
                        
            sleep(max(0, next_time - time()))
            tup = market_time_hk_futures()
            
            try:

                # Day Market
                if tup[0] == 'morning' or tup[0] == 'afternoon':
                    sleep_interval = self.config.refresh_interval_f
                    self.logger.debug('Futures {} in {} session, refresh: {}'.format(self.current_config_name, tup[0], sleep_interval))
                    self.refresh_and_push_futures()

                # if lunch hour, or mid-rest
                elif tup[0] == 'lunch' or tup[0] == 'mid-rest':
                    pass
    
                # Night Market
                elif tup[0] == 't_plus_one':
                    sleep_interval = self.config.refresh_interval_f
                    self.logger.debug('Night session {}, refresh: {}'.format(self.current_config_name, sleep_interval))
                    self.refresh_and_push_futures()

            except Exception:
                traceback.print_exc()

            delay = sleep_interval
            next_time += (time() - next_time) // delay * delay + delay

            # keep runnint
            keep_running = tup[1]


class StockBacktestPricer(StockPricer):

    def get_stock_quote(self, sym):
        """
        Define the stock pickle format passed


        This is no good to simulate tc data in here, coz
        current the backtesting may contain 1 day 1min only,
        not enought data to calc tc's data like Chg and Chgp

        """
        msg = copy(quote_dic)
        msg['product'] = 'Stocks'
        msg['sym'] = sym
        msg['1min'] = self.backtest_bars[sym].data1min[self.current_backtest_day].between_time("09:30:00", self.current_backtest_time)
        # dic['tc']   = {} #ba.tc.tc_rtq(sym, mode='full', return_dic=True)
        
        return msg

    def start_stock_pricer(self):
        """
    The backtest pricer
        """
        while True:
            if self.backtest_begin:
                if self.continue_backtest:
                    if self.current_backtest_time != self.prev_stock_backtest_time:
                        tup = market_time_keeper(now=self.current_backtest_time)
                        
                        if tup[0] == 'morning' or tup[0] == 'afternoon':
                            print('Stock', tup)
                            self.refresh_and_push_stock()
                            
                        self.prev_stock_backtest_time = self.current_backtest_time


class FuturesBacktestPricer(FuturesPricer):

    def get_futures_quote(self, sym):
        """
        Define the futures pickle format passed
        """
        
        msg = copy(quote_dic)
        msg['product'] = 'Futures'
        msg['sym'] = sym
        # dic['summary'] = {}  # ba.f.fquote(product= sym)
        msg['1min'] = self.backtest_bars[sym].data1min[self.current_backtest_day]\
            .between_time("09:15:00", self.current_backtest_time)

        return msg

    def start_futures_pricer(self):
        """
    The backtest pricer
        """
        while True:
            if self.backtest_begin:
                if self.continue_backtest:
                    if self.current_backtest_time != self.prev_futures_backtest_time:
                        
                        tup = market_time_keeper(now=self.current_backtest_time, product='futures')
                        
                        # --Day Market
                        if tup[0] == 'morning' or tup[0] == 'afternoon':
                            print('Futures', tup)
                            self.refresh_and_push_futures()
    
                        # --Night Market
                        elif tup[0] == 't_plus_one':
                            print('Futures', tup)
                            self.refresh_and_push_futures()
                            
                        self.prev_futures_backtest_time = self.current_backtest_time


class Pricer(FuturesPricer, StockPricer):
    """
    The design
    ---------------------------------
    Three Thread parallel running to handle task:
        1. Ticker subscribing
        2. Stock data push
        3. Futures data push
    """

    nth = 'first'


class PricerBacktest(FuturesBacktestPricer, StockBacktestPricer):
    """
    The design
    -------------------------
        Like TSCI, all database should be fully updated at the day end.
        When running backtest (or replay), we split data by day, into in-sample
        and out-sample data.
        The beneath simulation steps follow:
        1. split data
        2. pricer pretend to run during market (refresh period controled by config)
        3. Push whenever a subsbribe happens
        4. Action takes our data and calculate signals, push or trade like usual
        5. (implementing) Calculate performance after the simulation finish.

        ** Both backtest and replay using the same pricer **
    """
    
    def start_backtest(self):
        """
        start_backtest is just a ranger.
        Setting the time and sleep with interval.
        Those two backtest pricer will push the price base on simulate time.

        backtest_range is gernerated by : generate_datetime_range_and_backtest_bars
        """
        
        self.backtest_begin = True
        self.continue_backtest = True
        
        if not self.backtest_bars:
            self.logger.info('Backtest bars load fail')

        for rng in self.backtest_range:

            if self.continue_backtest:

                day = datetime.strftime(rng, "%Y%m%d")  # str: 20180101
                t = datetime.strftime(rng, "%Y-%m-%d %H:%M:%S")  # str
                tt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S").time()  # datetime.datetime

                if self.backtest_timer:
                    if tt > self.backtest_timer:
                        continue

                # self.logger.info('Current time is %s' % t)
                self.current_backtest_time = tt
                self.logger.info('Current backtest time is {}. symlist: {} flist: {}'.format(t, len(self.symlist), len(self.flist)))
                self.current_backtest_day = day


                # refresh the config
                self.config = ConfigManager('backtest')
                sleep(float(self.config.backtest_push_time))

        self.terminate_action()
        
    def stop_backtest(self):
        self.continue_backtest = False


if __name__ == '__main__':
    # =========================================================================
    # e.g. 
    # python pricer.py debug
    # python pricer.py production
    # python pricer.py backtest
    # =========================================================================
    parser = argparse.ArgumentParser(description='Pricer Module')
    parser.add_argument("model", default='spyder', nargs='?')  # read ini config
    args = parser.parse_args()

    dic = {'p': 'production',
           'd': 'debug',
           'b': 'backtest'}

    if args.model != 'b':
        pricer = Pricer(dic[args.model])
        pricer.create_socket()
        #pricer.create_stock_socket()
        #pricer.create_futures_socket()

        t1 = Thread(target=pricer.listen_for_work_message_request)
        t2 = Thread(target=pricer.start_futures_pricer)
        t3 = Thread(target=pricer.start_stock_pricer)

        t1.start()
        t2.start()
        t3.start()

    elif args.model == 'b':
        pricer = PricerBacktest('backtest')
        pricer.create_socket()
        #pricer.create_stock_socket()
        #pricer.create_futures_socket()
        
        t1 = Thread(target=pricer.listen_for_work_message_request)
        t2 = Thread(target=pricer.start_futures_pricer)
        t3 = Thread(target=pricer.start_stock_pricer)

        t1.start()
        t2.start()
        t3.start()
        #pricer.start_stock_pricer()
