# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 15:30:31 2018

@author: Branson

This is useless right now, mostly can replace by the config.ini
"""
from abc import abstractmethod, ABCMeta
from BATrader.market.MarketReader import currentTime, market_time_keeper
from BATrader.ileReader import read_ini_config
from time import strftime


class Heartbeat(object, metaclass=ABCMeta):

    @abstractmethod
    def heartbeat(self):
        raise NotImplementedError("Should implement heartbeat()")


class BacktestHearbeat(Heartbeat):

    def heartbeat(self):
        return 0.0


class LiveHeartbeat(object):

    def __init__(self, product='stock', config='production'):

        # config
        config = read_ini_config(config)

        if product == 'stock':
            c = config['heartbeat']

        self.rapid_interval = c['rapid']
        self.normal_interval = c['normal']
        self.refresh_interval = c['refresh']
        self.product = product

    def current_time(self):
        return int(strftime('%H%M'))

    def keeprunnng(self):
        """
        Booleen
        """
        tup = market_time_keeper()
        return tup[1]

    def heartbeat(self, callback_func):

        tup = market_time_keeper(product=self.product)  # return tuple, False when market closed

        # Day Market
        if tup[0] == 'pre_mkt':
            sleep_for = 5.0

        elif tup[0] == 'morning' or tup[0] == 'afternoon':
            # Rapid market 
            if 915 < self.current_time() <= 1030:
                sleep_for = self.rapid_interval
            elif 1300 <= self.current_time() <= 1330:
                sleep_for = self.rapid_interval
            # Normal Session
            else:
                sleep_for = self.normal_interval

        elif tup[0] == 'lunch':
            sleep_for = 5.0

        elif tup[0] == 'closed':
            print('Market Closed')
            return

        return sleep_for


if __name__ == '__main__':
    heartbeat = LiveHeartbeat()
    print(heartbeat.keeprunnng())
    print(heartbeat.heartbeat())
