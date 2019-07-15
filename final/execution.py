# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 11:50:36 2015

@author: Branson
"""

from abc import ABCMeta, abstractmethod
import datetime

from BATrader.logger import Logger
import BATrader as ba

from BATrader.final.event import FillEvent
from BATrader.data.futures import Futures
from BATrader.fileReader import read_ini_config

import zmq
from threading import Thread

from BATrader.final.config_manager import ConfigManager

import json

# =============================================================================
# Commission Handler
# =============================================================================


config = read_ini_config('commission')


def transaction_cost(p, q, broker='bsgroup'):
    from math import ceil
    mini = float(config[broker]['min'])
    comm = float(config[broker]['comm']) / 100
    levy = float(config[broker]['levy']) / 100
    trans = float(config[broker]['trans']) / 100
    ccass = float(config[broker]['ccass']) / 100
    ccass_min = float(config[broker]['ccass_min'])
    stamp = float(config[broker]['stamp']) / 100
    # margin_rate = float(config[broker]['margin_rate']) / 100

    # comm?
    turnover = p * q
    # using ternary a if condition else b
    commission = (turnover * comm) if (turnover * comm) > mini else mini
    trading_levy = turnover * levy
    transaction_fee = turnover * trans
    ccass_fee = (turnover * ccass) if (turnover * ccass) > ccass_min else ccass_min
    stamp_duty = ceil(turnover * stamp)

    transaction_cost = commission + trading_levy + transaction_fee + ccass_fee + stamp_duty
    return transaction_cost


def transaction_cost_futures(q, product='hsif', broker='bsgroup'):
    product = str.lower(product)
    if product == 'hsif' or product == 'mhif':
        comm = float(config[broker]['comm_%s' % product])
        sfc = float(config[broker]['sfc_%s' % product])
        trans = float(config[broker]['trans_%s' % product])

        transaction_cost = (comm + sfc + trans) * int(q)
        return transaction_cost
    else:
        print('product no found')
        return 0


def futures_multipier(product='hsif'):
    product = str.lower(product)
    dic = {'hsif': 50,
           'mhif': 10, }
    return dic[product]


# =============================================================================
# Execution Handler
# =============================================================================
class ExecutionHandler(object, metaclass=ABCMeta):

    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError("Should implement execute_order()")


class SimulatedExecutionHandler(ExecutionHandler):
    """
    最簡單的模擬執行
    Designed for single product, and act like echo
    """

    def __init__(self, events, portfolio, logger, **kwargs):
        print("Init SimulatedExecutionHandler")
        self.events = events
        self.portfolio = portfolio
        self.logger = logger

    def execute_order(self, order_event):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        Parameters:
        event - Contains an Event object with order information.
        """
        print("execute_order")
        if order_event.type == 'ORDER':
            fill_event = FillEvent(
                datetime.datetime.utcnow(), order_event.sym,
                'HKEX', order_event.quantity, order_event.direction, None
            )
            self.events.put(fill_event)


class TraderExecutionHandler(ExecutionHandler):
    """
    Paper trade at first
    """

    def __init__(self, events, portfolio, logger, **kwargs):

        print("Init TraderExecutionHandler")
        self.events = events
        self.portfolio = portfolio
        self.logger = logger

        # Commission
        self.commission_scheme = self.broker = 'bsgroup'
        self.slippage = 0

    # product, timeindex, sym, quantity, 
    # direction, fill_cost, commission=None, 
    # broker='bsgroup', slippage= 0)
    def execute_order(self, order_event):

        if order_event.type == 'ORDER':
            print("Got an order from trader")

            # execute trade
            if order_event.product == 'stocks':

                # check current position
                current_position = self.portfolio.current_positions[order_event.sym]

                fill_cost = order_event.price * order_event.quantity
                profit = 0

                commission = transaction_cost(order_event.price, order_event.quantity, self.broker)
                fill_event = FillEvent(
                    datetime.datetime.utcnow(),  # TODO: Check if this is fine to use current time
                    order_event.direction,
                    order_event.sym,
                    order_event.quantity,
                    order_event.price,
                    fill_cost,
                    commission,
                    order_event.product,
                    profit
                )
                self.events.put(fill_event)

            # futures
            elif order_event.product == 'futures':

                # check current position
                current_position = self.portfolio.current_positions[order_event.sym]
                current_fill_price = self.portfolio.fill_price[order_event.sym]
                margin = Futures.margin(order_event.sym)  # 開倉 margin

                if current_position == 0:
                    print("Now open a new position")

                    fill_cost = margin * order_event.quantity
                    profit = 0

                    # calc comission
                    print('calculate commission')
                    commission = transaction_cost_futures(order_event.quantity, order_event.sym, self.commission_scheme)
                    print(('commission is %s' % commission))

                    # ----------------- AFter Fill in real world -----------------
                    # create fill event of new position
                    fill_event = FillEvent(
                        datetime.datetime.utcnow(),
                        order_event.direction,
                        order_event.sym,
                        order_event.quantity,
                        order_event.price,
                        fill_cost,
                        commission,
                        order_event.product,
                        profit
                    )
                    print('sending fill event')
                    self.events.put(fill_event)

                else:
                    if current_position < 0 and order_event.direction == 'BUY':
                        print('now square a short position')
                    elif current_position > 0 and order_event.direction == 'SELL':
                        print('now square a long position')
                    else:
                        print('adding position')

                    # calc comission
                    print('calculate commission')
                    commission = transaction_cost_futures(order_event.quantity, order_event.sym, self.commission_scheme)
                    print(('commission is %s' % commission))

                    multipier = futures_multipier(order_event.sym)
                    fill_cost = margin + (((order_event.price - current_fill_price) * order_event.quantity) * multipier)

                    # ( current position X (( new fill price - current fill price) X multipier) )
                    pnl = (current_position * ((
                                                           order_event.price - current_fill_price) * multipier))  # since it's hard to include the commission of open position, so we don't deduct here

                    print(("PnL: %s" % (pnl - commission)))
                    # create fill event
                    fill_event = FillEvent(
                        datetime.datetime.utcnow(), order_event.direction, order_event.sym,
                        order_event.quantity, order_event.price, fill_cost, commission, order_event.product, pnl
                    )
                    print('sending fill event')
                    self.events.put(fill_event)


class SpExecutionHandler(ExecutionHandler):
    """
    Trader to make a execution handler with can communicated with sp_server
    """

    def __init__(self, events='', portfolio='', logger='', **kwargs):

        print("Init TraderExecutionHandler")
        # logger
        self.logger = Logger(logname=ba.fr.path_output + 'logger\\SP_Trader_%s.log' % ba.dk.tradeday(),
                             logger='sp').getlog()

        self.events = events
        self.portfolio = portfolio
        self.logger = logger
        
        # Commission
        self.commission_scheme = self.broker = 'bsgroup'
        self.slippage = 0

        self.config = ConfigManager('debug', 'trading')

    def connect_sp_server(self):

        tcp = "tcp://%s:%s" % (self.config.trading_server_ip, self.config.req_rep_port)

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(tcp)

        socket.send_pyobj({'msg': 'Execution Handler is connected'})
        response = socket.recv_json()
        print(response['msg'])

        self.socket = socket

    def make_request(self, req_msg):

        self.socket.send_pyobj(req_msg)
        response = self.socket.recv_json()
        print("Server sponse: %s" % response['msg'])

    def pull_server_and_callback(self):
        """
        Pull from server push. Then callback
        在這裡處理 call back
        Returns:

        """
        tcp = "tcp://%s:%s" % (self.config.trading_server_ip, self.config.push_pull_port)

        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.connect(tcp)

        # threading
        def pull_msg():
            while True:
                # a message came
                response = socket.recv_pyobj()

                # Message is not empty, get it and push
                self.logger.debug('Server push us a callback message')
                self.logger.debug(response)
                
                # connection status
                
                # if order is filled or any error, make a fill event


        th = Thread(target=pull_msg)
        th.start()

    def execute_order(self, order_event):

        self.logger.debug('Sending order to SP Server')
        
        # pass the order
        self.make_request({'order': order_event})
        
    def is_sp_server_alive_and_connected(self):
        
        # pass the order
        self.make_request({'cmd': 'alive_bool'})



if __name__ == "__main__":
    
    from event import OrderEvent
    order = OrderEvent.dump()
    
    ex = SpExecutionHandler()
    ex.connect_sp_server()
    ex.pull_server_and_callback()
    
    # ex.execute_order(order)
