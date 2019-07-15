#!/usr/bin/python
# -*- coding: utf-8 -*-

class Event():
    pass


class MarketEvent(Event):

    def __init__(self, product, sym, last_bar):
        self.type = 'MARKET'
        self.product = product
        self.sym = sym
        self.last_bar = last_bar  # dict


class RefreshEvent(Event):

    def __init__(self):
        self.type = 'REFRESHED'


class SignalEvent(Event):
    """
    ['strategy_id','datetime','signal_type','sym',' price', 'quantity', 'strength']
    """

    def __init__(self, strategy_id, datetime, signal_type,
                 sym, price, quantity, strength, product):
        self.type = 'SIGNAL'
        self.strategy_id = strategy_id
        self.datetime = datetime
        self.signal_type = signal_type
        self.sym = sym
        self.price = price
        self.quantity = quantity
        self.strength = strength
        self.product = product

    @classmethod
    def create_signal_by_dic(cls):
        pass

    def print_signal(self):
        print(
            "Order: s_id=%s, datetime=%s, signal_type=%s, sym=%s, price=%s, quantity=%s, strength=%s, product=%s" %
            (self.strategy_id, self.datetime, self.signal_type, self.sym, self.price, self.quantity, self.strength,
             self.product)
        )


class MonitorEvent(Event):
    
    def __init__(self, header_msg):
        self.type = 'MONITOR'
        self.signal_name_list = []
        self.msg_list = [header_msg]
        self.msg_for_send = ''
        self.sym = ''

    def add_monitor_msg(self, time, signal_name, sym, chi_name, price, chgp):
        self.sym = sym
        add_msg = '{} {}% {} {}'.format(time, chgp, price, signal_name)
        self.msg_list.append(add_msg)
        self.signal_name_list.append(signal_name)
        self.msg_for_send = '\n'.join(self.msg_list)

    def print_monitor_msg(self):
        for m in self.msg_list:
            print(m)


class OrderEvent(Event):

    def __init__(self, direction, sym, price, quantity, order_type, product):
        # ['B/S', 'sym', 'price', 'quantity', 'order_type']
        self.type = 'ORDER'
        self.direction = direction
        self.sym = sym
        self.price = price
        self.quantity = quantity
        self.order_type = order_type
        self.product = product

    def print_order(self):
        print(
            "Order: direction=%s, sym=%s, price=%s, quantity=%s, order_type=%s, product=%s" %
            (self.direction, self.sym, self.price, self.quantity, self.order_type, self.product)
        )
        
    @classmethod
    def dump(cls):
        return cls('B', 'HSIN9', 28000, 1, 'MKT', 'futures')


class FillEvent(Event):

    def __init__(self, timeindex, direction, sym,
                 fill_quantity, fill_price,
                 fill_cost, commission, product, pnl=0):
        # print("Init Fill event class")
        self.type = 'FILL'
        self.timeindex = timeindex
        self.direction = direction
        self.sym = sym

        self.fill_quantity = fill_quantity
        self.fill_price = fill_price
        self.fill_cost = fill_cost
        self.commission = commission
        self.product = product
        self.pnl = pnl

    def print_fill(self):
        print(
            "Order: Time=%s, direction=%s, sym=%s, fill_quantity=%s, fill_price=%s, fill_cost=%s,  commission=%s, product=%s, pnl=%s" %
            (self.timeindex, self.direction, self.sym, self.fill_quantity, self.fill_price, self.fill_cost,
             self.commission, self.product, self.pnl)
        )
