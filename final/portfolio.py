# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 10:08:16 2015

@author: Branson
"""


import datetime
from math import floor
import queue

import numpy as np
import pandas as pd

from BATrader.final.event import FillEvent, OrderEvent
from BATrader.final.performance import create_sharpe_ratio, create_drawdowns
from collections import defaultdict


class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.

    The positions DataFrame stores a time-index of the 
    quantity of positions held. 

    The holdings DataFrame stores the cash and total market
    holdings value of each sym for a particular 
    time-index, as well as the percentage change in 
    portfolio total across data_handler.
    """

    def __init__(self, data_handler, events, initial_capital, **kwargs):
        """
        Initialises the portfolio with data_handler and an event queue. 
        Also includes a starting datetime index and initial capital 
        (USD unless otherwise stated).

        Parameters:
        data_handler - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.data_handler = data_handler
        self.events = events
        self.symbol_list = self.data_handler.symbol_list
        self.start_date = self.data_handler.params['startday']
        self.initial_capital = initial_capital
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        return [d]

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).

        Makes use of a MarketEvent from the events queue.
        """
        latest_datetime = self.data_handler.get_latest_bar_datetime(self.symbol_list[0])

        # Update positions
        # ================
        dp = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        dp['datetime'] = latest_datetime

        for s in self.symbol_list:
            dp[s] = self.current_positions[s]

        # Append the current positions
        self.all_positions.append(dp)

        # Update holdings
        # ===============
        dh = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * \
                           self.data_handler.get_latest_bar_value(s, "Close")
            dh[s] = market_value
            dh['total'] += market_value

        # Append the current holdings
        self.all_holdings.append(dh)

    # ======================
    # FILL/POSITION HANDLING
    # ======================

    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.

        Parameters:
        fill - The Fill object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'B':
            fill_dir = 1
        if fill.direction == 'S':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill.sym] += fill_dir*fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.

        Parameters:
        fill - The Fill object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'B':
            fill_dir = 1
        if fill.direction == 'S':
            fill_dir = -1

        # Update holdings list with new quantities
        fill_cost = self.data_handler.get_latest_bar_value(
            fill.sym, "Close"
        )
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.sym] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
        Simply files an Order object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The tuple containing Signal information.
        """
        order = None

        sym = signal.sym
        direction = signal.signal_type
        strength = signal.strength

        mkt_quantity = 100
        cur_quantity = self.current_positions[sym]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(sym, order_type, mkt_quantity, 'B')
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(sym, order_type, mkt_quantity, 'S')   
    
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(sym, order_type, abs(cur_quantity), 'S')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(sym, order_type, abs(cur_quantity), 'B')
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    # ========================
    # POST-BACKTEST STATISTICS
    # ========================

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns, periods=252*60*6.5)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve['drawdown'] = drawdown

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('equity.csv')
        return stats


class BacktestPortfolio(object):
    """    
    2019-07
        回測的Portfolio
    
    """

    def __init__(self, data_handler, events, logger, insample_end_day, **kwargs):
        """
        Parameters:
            data_handler - The DataHandler object with current market data.
            events - The Event Queue object.
            start_date - The start date (bar) of the portfolio.
            initial_capital - The starting capital in USD.
        """
        self.params_dict = kwargs
        
        self.data_handler = data_handler
        self.events = events
        self.logger = logger
        self.tradeday = self.start_date = insample_end_day
        
        # Sometimes we don't actually need the portfolio class
        if 'initial_capital' in self.params_dict:
            self.initial_capital = kwargs['initial_capital']
        else:
            self.initial_capital = 0

        # Holding the current bar datetime
        self.latest_bar_datetime = None

        self.symbol_list = []

    def construct_portfolio(self, symbol_list):
        """
        2019-07 
            To init the portfolio. 
            If live trading, should get the portfolio record from 
        """
        if not self.symbol_list:
            self.logger.debug('Constructing Portfolio')
            self.symbol_list = symbol_list

            self.all_positions = self.construct_all_positions()
            self.current_positions = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])

            self.all_holdings = self.construct_all_holdings()
            self.current_holdings = self.construct_current_holdings()

            self.fill_price = dict((k, v) for k, v in [(s, 0.0) for s in self.symbol_list])  # for futures

    def construct_by_sym(self, sym):
        if sym not in self.symbol_list:
            self.symbol_list.append(sym)
            self.all_positions.append({'datetime': self.start_date, sym: 0})
            self.current_positions[sym] = 0

            self.all_holdings[0][sym] = 0
            self.current_holdings[sym] = 0.0

            self.fill_price[sym] = 0.0

    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d]

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict((k, v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
        d = dict((k, v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).

        Makes use of a MarketEvent from the events queue.
        """
        sym = event.sym
        latest_datetime = self.data_handler.get_latest_bar_datetime(sym)

        # Check all holdings and all positions time index, if not exists, create
        if self.latest_bar_datetime != latest_datetime:
            # Update positions
            # ================
            dp = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])  # TODO: BUG!?
            dp['Date'] = latest_datetime

            for s in self.symbol_list:
                dp[s] = self.current_positions[s]

            # Append the current positions
            self.all_positions.append(dp)

            # Update holdings
            # ===============
            dh = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
            dh['Date'] = latest_datetime
            dh['cash'] = self.current_holdings['cash']
            dh['commission'] = self.current_holdings['commission']
            dh['total'] = self.current_holdings['cash']

            # for s in self.symbol_list:
            # Approximation to the real value
            for sym in self.symbol_list:
                try:
                    market_value = self.current_positions[sym] * self.data_handler.get_latest_bar_value(sym, "Close")
                except:
                    continue
                else:
                    dh[sym] = market_value
            dh['total'] += market_value

            # Append the current holdings
            self.all_holdings.append(dh)

            self.latest_bar_datetime = latest_datetime

        # if exists, just update the market price
        else:
            # Update positions
            self.all_positions[-1][sym] = self.current_positions[sym]
           
            # Update holdings
            
            # 目前持貨市值
            #for sym in self.symbol_list:
            
            self.all_holdings[-1][sym] = self.current_positions[sym] * self.data_handler.get_latest_bar_value(sym, "Close")
            self.all_holdings[-1]['cash'] = self.current_holdings['cash']
            self.all_holdings[-1]['commission'] = self.current_holdings['commission']
            self.all_holdings[-1]['total'] = self.current_holdings['total']

    # ======================
    # FILL/POSITION HANDLING
    # ======================

    def update_positions_from_fill(self, fill_event):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.

        Parameters:
        fill - The Fill object to update the positions with.
        """
        self.logger.debug('update_positions_from_fill')
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill_event.direction == 'B':
            fill_dir = 1
        if fill_event.direction == 'S':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill_event.sym] += fill_dir * fill_event.fill_quantity

    def update_holdings_from_fill(self, fill_event):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.

        Parameters:
            fill_event - The Fill object to update the holdings with.
        
        fill_event:
            fill_quantity=1000, fill_price=8, fill_cost=8000,  
            commission=63.616, product=stocks, pnl=0
        """
        self.logger.debug('update_holdings_from_fill')
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill_event.direction == 'B':
            fill_dir = 1
        if fill_event.direction == 'S':
            fill_dir = -1



        # Update holdings list with new quantities
        fill_total_cost = fill_event.fill_cost + (fill_dir * fill_event.commission)
        
        mkt_price = self.data_handler.get_latest_bar_value(
            fill_event.sym, "Close"
        )
        
        # if no positions, current is zero
        if not self.current_positions[fill_event.sym]:
            self.current_holdings[fill_event.sym] = 0.0
        else:
            self.current_holdings[fill_event.sym] += (mkt_price * self.current_positions[fill_event.sym])
        self.current_holdings['commission'] += fill_event.commission
        self.current_holdings['cash'] -= (fill_total_cost * fill_dir)
        
        # 目前持貨總值 = 目前持股市值 + 手上現金
        stocks_mkt_value = 0
        for sym in self.symbol_list:
            stocks_mkt_value += self.current_holdings[sym]
        
        self.current_holdings['total'] = stocks_mkt_value + self.current_holdings['cash']

    def update_fill(self, fill_event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if fill_event.type == 'FILL':
            self.update_positions_from_fill(fill_event)
            self.update_holdings_from_fill(fill_event)
            self.update_timeindex(fill_event)

    def generate_naive_order(self, signal_event):
        """
        Simply files an Order object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The tuple containing Signal information.
        
        """
        order = None
        # SingalEvent -> OrderEvent
        # OrderEvent: direction, sym, price, quantity, order_type, product
        
        strategy_id = signal_event.strategy_id
        datetime = signal_event.datetime
        direction = signal_event.signal_type
        sym = signal_event.sym
        price = signal_event.price
        quantity = signal_event.quantity
        strength = signal_event.strength
        product = signal_event.product
        
        # mkt_quantity = 100
        cur_quantity = self.current_positions[sym]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent('B', sym, price, quantity, order_type, product)
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent('S', sym, price, quantity, order_type, product)
    
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent('S', sym, price, quantity, order_type, product)
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent('B', sym, price, quantity, order_type, product)
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        Called first by backtest.py when siganl event received.
        """
        if event.type == 'SIGNAL':
            self.logger.debug('Got signal event, gen order now')
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    # ========================
    # POST-BACKTEST STATISTICS
    # ========================

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('Date', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns, periods=252*60*6.5)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve['drawdown'] = drawdown

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('equity.csv')
        return stats
    
    def print_portfolio(self):
        print('--------- Current Holdings :--------- ')
        print(self.current_holdings)
        print('--------- All holdings :--------- ')
        print(self.all_holdings[-1])
        
        print('--------- Current positions :--------- ')
        print(self.current_positions)
        print('--------- All positions :--------- ')
        print(self.all_positions[-1])
        print()
        
#        print('--------- Fill price :--------- ')
#        print(self.fill_price)
 
class LivePortfolio(BacktestPortfolio):
    """
    This will be used in Production and Debug
    """
    def __init__(self, data_handler, events, logger, tradeday, **kwargs):
        
        # Init base class
        super(LivePortfolio, self).__init__(data_handler, events, logger, tradeday, **kwargs)
        
        self.symbol_list = []
        
        # Some hodlings ?
        self.all_positions     = [] # use by update_timeindex, show all history
        self.current_positions = {} 
        self.all_holdings      = [] # use by update_timeindex
        self.current_holdings  = {}
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = defaultdict(lambda:0) #dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        self.all_holdings     = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()
        
        self.fill_price = defaultdict(lambda:0) # dict( (k,v) for k, v in [(s, 0.0) for s in init_symbol_list]) # for futures 
        
    def construct_by_sym(self, sym):
        if sym not in self.symbol_list:
            self.symbol_list.append(sym)
            self.all_positions.append({'datetime':self.start_date, sym: 0})
            self.current_positions[sym] = 0
            
            self.all_holdings[0][sym] = 0
            self.current_holdings[sym] = 0.0
            
            self.fill_price[sym] = 0.0
        
    def construct_all_positions(self):

        #d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        #d['datetime'] = self.tradeday
        d = defaultdict(lambda: 0)
        return [d]

    def construct_all_holdings(self):

        #d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
#        d['datetime'] = self.start_date
#        d['cash'] = self.initial_capital
#        d['commission'] = 0.0
#        d['total'] = self.initial_capital
        d = defaultdict(lambda: 0.0)
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self):

#        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
#        d['cash'] = self.initial_capital
#        d['commission'] = 0.0
#        d['total'] = self.initial_capital
        d = defaultdict(lambda: 0.0)
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).

        Makes use of a MarketEvent from the events queue.
        """
        sym = event.sym
        
        #print("update_timeindex")
        latest_datetime = self.data_handler.get_latest_bar_datetime(sym)

        # Update positions
        # ================
        dp = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dp['Date'] = latest_datetime

        #for s in self.symbol_list:
        dp[sym] = self.current_positions[sym]

        # Append the current positions
        self.all_positions.append(dp)

        # Update holdings
        # ===============
        dh = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dh['Date'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        #for s in self.symbol_list:
        
        # Approximation to the real value
        market_value = self.current_positions[sym] * self.data_handler.get_latest_bar_value(sym, "Close")
        dh[sym] = market_value
        dh['total'] += market_value

        # Append the current holdings
        self.all_holdings.append(dh)

    # ======================
    # FILL/POSITION HANDLING
    # ======================

    def update_positions_from_fill(self, fill_event):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.

        Parameters:
        fill - The Fill object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill_event.direction == 'B':
            fill_dir = 1
        if fill_event.direction == 'S':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill_event.sym] += fill_dir * fill_event.fill_quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.

        Parameters:
        fill - The Fill object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'B':
            fill_dir = 1
        if fill.direction == 'S':
            fill_dir = -1

        # Update holdings list with new quantities
        fill_cost = fill.fill_cost
        cost = fill_dir * fill_cost # * fill.fill_quantity
        self.current_holdings[fill.sym] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)
        
        if fill.product == 'futures':
            self.fill_price[fill.sym] = fill.fill_price
        
        if self.current_positions[fill.sym] == 0: # should make it 0 if squared. if not, the holdings will be wrong (e.g. -1500 when earn 30pts)
            self.current_holdings[fill.sym] = 0
            self.fill_price[fill.sym] = 0.0
        


    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
        Simply files an Order object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The tuple containing Signal information.
        
        """
        order = None

        
        strategy_id = signal.strategy_id
        datetime = signal.datetime
        direction = signal.signal_type
        sym = signal.sym
        price = signal.price
        quantity = signal.quantity
        strength = signal.strength
        product = signal.product

        #mkt_quantity = signal.quantity
        cur_quantity = self.current_positions[sym]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent('B', sym, price, quantity, order_type, product)
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent('S', sym, price, quantity, order_type, product)   
    
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent('S', sym, price, abs(cur_quantity), order_type, product)
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent('B', sym, price, abs(cur_quantity), order_type, product)
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        Called first by backtest.py when siganl event received.
        """
        if event.type == 'SIGNAL':
            self.construct_by_sym(event.sym)
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    # ========================
    # POST-BACKTEST STATISTICS
    # ========================

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('Date', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns, periods=252*60*6.5)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve['drawdown'] = drawdown

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('equity.csv')
        return stats


class LivePortfolio_old(Portfolio):

    def __init__(self, data_handler, events, logger, insample_end_day, initial_capital, **kwargs):
    #def __init__(self, events, initial_capital=100000.0):

        self.events = events
        self.initial_capital = initial_capital

        self.symbol_list = ['HSIF']

        #        self.all_positions = self.construct_all_positions()
        #        self.current_positions = defaultdict(lambda:0) #dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        #
#        self.all_holdings     = self.construct_all_holdings()
#        self.current_holdings = self.construct_current_holdings()
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = defaultdict(lambda:0) #dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        self.all_holdings     = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()
        
        self.fill_price = defaultdict(lambda:0)

    def construct_all_positions(self):
#        """
#        Constructs the positions list using the start_date
#        to determine when the time index will begin.
#        """
        #d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        #d['datetime'] = self.start_date
        # {'1': 0, '3': 0, '5': 0, '9': 0, 'datetime': '20180816'}
        d = defaultdict(lambda: 0)
        return [d]

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        #d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
#        d['datetime'] = self.start_date
#        d['cash'] = self.initial_capital
#        d['commission'] = 0.0
#        d['total'] = self.initial_capital
        d = defaultdict(lambda: 0.0)
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        
        return [d]

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
#        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
#        d['cash'] = self.initial_capital
#        d['commission'] = 0.0
#        d['total'] = self.initial_capital
        d = defaultdict(lambda: 0.0)
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d


    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).

        Makes use of a MarketEvent from the events queue.
        """
        #print("update_timeindex")
        latest_datetime = event.bar['Datetime'] if 'Datetime' in event.bar else event.bar['Date']

        # Update positions
        # ================
        dp = dict( (k,v) for k, v in [(s, 0) for s in [event.sym]] )
        dp['Date'] = latest_datetime

        for s in [event.sym]:
            dp[s] = self.current_positions[s]

        # Append the current positions
        self.all_positions.append(dp)

        # Update holdings
        # ===============
        dh = dict( (k,v) for k, v in [(s, 0) for s in [event.sym]] )
        dh['Date'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * \
                event.bar['Close']
            dh[s] = market_value
            dh['total'] += market_value

        # Append the current holdings
        self.all_holdings.append(dh)
        print('')

    # ======================
    # FILL/POSITION HANDLING
    # ======================

    def update_positions_from_fill(self, fill): #倉位
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.

        Parameters:
        fill - The Fill object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'B':
            fill_dir = 1
        if fill.direction == 'S':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill.sym] += fill_dir*fill.quantity

    def update_holdings_from_fill(self, fill): #銀彈
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.

        Parameters:
        fill - The Fill object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'B':
            fill_dir = 1
        if fill.direction == 'S':
            fill_dir = -1

        # Update holdings list with new quantities
#        fill_cost = self.data_handler.get_latest_bar_value(
#            fill.sym, "Close"
#        )
        fill_cost = fill.fill_cost + fill.slippage
        
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.sym] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, fill_event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if event.type == 'FILL':

            if event.product == 'stocks':
                # Check whether the fill is a buy or sell
                fill_dir = 0
                if fill.direction == 'B':
                    fill_dir = 1
                if fill.direction == 'S':
                    fill_dir = -1

                # Update positions list with new quantities
                self.current_positions[fill.sym] += fill_dir * fill.quantity

                # Check whether the fill is a buy or sell
                fill_dir = 0
                if fill.direction == 'B':
                    fill_dir = 1
                if fill.direction == 'S':
                    fill_dir = -1

                fill_cost = fill.fill_cost + fill.slippage

                cost = fill_dir * fill_cost * fill.quantity
                self.current_holdings[fill.sym] += cost
                self.current_holdings['commission'] += fill.commission
                self.current_holdings['cash'] -= (cost + fill.commission)
                self.current_holdings['total'] -= (cost + fill.commission)



            elif event.product == 'futures':

                fill = event

                print('Update fill event received, now update:')
                fill.print_fill()

                # 倉位
                fill_dir = 0
                if fill.direction == 'B':
                    fill_dir = 1
                elif fill.direction == 'S':
                    fill_dir = -1

                # Update positions list with new quantities
                self.current_positions[fill.sym] += fill_dir * fill.fill_quantity

                # 銀彈
                fill_dir = 0
                if fill.direction == 'B':
                    fill_dir = 1
                elif fill.direction == 'S':
                    fill_dir = -1

                cost = fill_dir * fill.fill_cost  # 1 * 133,337  / -1 * 133,337
                self.current_holdings[fill.sym] += cost
                self.current_holdings['commission'] += fill.commission
                self.current_holdings['cash'] -= (
                            cost + fill.commission)  # cash will be larger when short, so should regard to total
                if self.current_positions[
                    fill.sym] == 0:  # should make it 0 if squared. if not, the holdings will be wrong (e.g. -1500 when earn 30pts)
                    self.current_holdings[fill.sym] = 0
                self.current_holdings['total'] = self.current_holdings['cash'] + self.current_holdings[fill.sym]
                self.current_holdings['pnl'] += (fill.pnl - fill.commission)
                self.fill_price[fill.sym] = fill.fill_price

                print('')

    def generate_naive_order(self, signal, double_up_allowed=True):
        """
        Generate order when got signal
        """
        print('Generate order')
        order = None

        datetime = signal.datetime
        direction = signal.signal_type
        sym = signal.sym
        price = signal.price
        quantity = signal.quantity
        strength = signal.strength
        product = signal.product

        print((datetime, direction, sym, price, quantity, strength, product))
        cur_quantity = self.current_positions[sym]

        order_type = 'MKT'

        # 開倉 / 平倉
        if double_up_allowed:  # 可以加倉
            if direction == 'LONG':
                order = OrderEvent('B', sym, price, quantity, order_type, product)
            if direction == 'SHORT':
                order = OrderEvent('S', sym, price, quantity, order_type, product)
        else:  # 不可以加倉, 只可平倉
            if direction == 'LONG' and cur_quantity == 0:
                order = OrderEvent('B', sym, price, quantity, order_type, product)
            if direction == 'SHORT' and cur_quantity == 0:
                order = OrderEvent('S', sym, price, quantity, order_type, product)

                # 完全平倉
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent('S', sym, price, abs(cur_quantity), order_type, product)
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent('B', sym, price, abs(cur_quantity), order_type, product)

        order.print_order()

        return order
    
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        Called first by backtest.py when siganl event received.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    # ========================
    # POST-BACKTEST STATISTICS
    # ========================

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('Date', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns, periods=252*60*6.5)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve['drawdown'] = drawdown

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('equity.csv')
        return stats

