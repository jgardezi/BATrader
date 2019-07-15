# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 09:35:35 2018

@author: Branson
"""
import BATrader as ba
from BATrader.market.MarketReader import convert
import pandas as pd

from functools import partial


class Filters(object):
    """
     FIlter
     ------------------------------------------
         Filter is used when I can told you immediately when you ask for specific
         group of stock. This Filter operate symlist in postgresql and deliver
         dataframe on row.

    Usage:
        
        self.df  = ba.fr.symlist
        self.df1 = filter_by_mkt_value((0,'1B'))
        self.df2 = filter_by_entry_cost('10K')

        self.df3 = multiple_condition_filter(filter_by_mkt_value(('300M',0)),
                                  filter_by_entry_cost('10K'),
                                  filter_by_over_ma_60())

        d = self.df.query("cap_in_num > 500000000 and (board == 'main' or board == 'gem')")     # using query

    # Return active stock only
        ba.fr.symlist = filter_by_status(1)
        
    NewStyle:
        cond1 = stock['毛利率(%)'] > 30
        cond2 = stock['營業利益率(%)'] > 30
        stock[cond1 & cond2]

    """

    def __init__(self):
        self.df = ba.fr.init_symlist_once()

    def filter_by_symlist(self, symlist: list) -> pd.DataFrame:
        """
        filter by symlist, 931 µs 
        """
        return self.df[self.df.sym.isin(symlist)]
    
    def filter_by_symlist2(self, symlist: list) -> pd.DataFrame:
        """
        filter by symlist, slower.....4.22 ms 
        """
        return self.df.set_index('sym').loc[symlist].reset_index()

    def filter_by_chi_name_contain(self, chi_word: str) -> pd.DataFrame:
        """
        filter by chinese name contains
        """
        return self.df[self.df.chi_name.str.contains(chi_word)]

    def filter_by_status(self, status: int) -> pd.DataFrame:
        """
        filter by stock trading status.
        to simplify,
            1 is 'Active', 0 is 'Suspended'
        """
        dic = {1: 'Active', 0: 'Suspended'}

        return self.df[self.df.status == dic[status]]

    def filter_by_over_ma(self, period) -> pd.DataFrame:

        return self.df[self.df.pClose > self.df['ma%s' % period]]

    def filter_by_over_ma_60(self) -> pd.DataFrame:

        return self.filter_by_over_ma(60)

    def filter_by_over_ma_200(self) -> pd.DataFrame:

        return self.filter_by_over_ma(200)

    def filter_by_entry_cost(self, entry_cost) -> pd.DataFrame:
        """
        filter by 每手入場費, coz always below entry cost we need so didn't use range
            entry_cost = boardlot x p.close
                int or str : directly
                tuple : high-land, low-land
        """

        self.df = ba.fr.symlist
        self.df['entry_cost'] = self.df.boardlot * self.df.pClose
        self.df = self.df[self.df.entry_cost.notnull()]

        if type(entry_cost) == tuple:
            return self.df[
                (self.df.entry_cost > convert(entry_cost[0])) & (self.df.entry_cost < convert(entry_cost[1]))]
        else:
            return self.df[self.df.entry_cost < convert(entry_cost)]

    def filter_by_board(self, board: str) -> pd.DataFrame:
        """
        filter by input board
        """

        if type(board) != str:
            raise TypeError("main?gem?etf?")

        return self.df[self.df.board == board]

    def filter_by_mkt_value(self, mkt_value: tuple) -> pd.DataFrame:
        """
        filter by market value in range
        must be a tuple : (0, '500M')
            30億以上: -> ('300M', 0)
        """

        if type(mkt_value) != tuple:
            raise TypeError("tuple : high-land, low-land")

        if mkt_value[1] == 0:
            return self.df[self.df.cap_in_num > convert(mkt_value[0])]
        else:
            return self.df[(self.df.cap_in_num > convert(mkt_value[0])) & (self.df.cap_in_num < convert(mkt_value[1]))]

    def filter_by_amount(self, amount: float) -> pd.DataFrame:
        """
        filter by amount (Turnover = P x Q)
            must be a flow : (0.0)
        """
        return self.df[(self.df.pAmount > amount)]

    def filter_by_trade_price(self, bigger_or_smaller_sign, trade_price) -> pd.DataFrame:
        """
        filter by Traded price(Last or Close)
        """

        if type(trade_price) != float and type(trade_price) != int:
            raise ValueError("int or float only : 0.0 or 0")
        if bigger_or_smaller_sign == '>':
            return self.df[(self.df.pClose > trade_price)]
        if bigger_or_smaller_sign == '<':
            return self.df[(self.df.pClose < trade_price)]

    def filter_ot_list(self) -> pd.DataFrame:

        return self.df[self.df.ot_and_rubbish == '1']
    # ------ aastocks ------
    def filter_by_aa_sector(self, sector):
        """
        Better update the aa sector regularly
        """
        return self.df.query("aa_sector == '{}'".format(sector))
    
    def filter_by_aa_business(self, business):
        """
        Better update the aa sector regularly
        """
        return self.df.query("aa_business == '{}'".format(business))
    # ------ Other ------
    @staticmethod
    def merge_everything(df1, df2) -> pd.DataFrame:

        cols_to_use = df2.columns.difference(df1.columns)
        df_ = pd.merge(df1.set_index('sym'), df2.set_index('sym')[cols_to_use], left_index=True, right_index=True,
                       how='inner').reset_index()
        return df_

    def multiple_condition_filter(self, *df_hub) -> pd.DataFrame:
        """
        pass in multiple condition and return,
        this looks complex but it's very convenient.
        
        self.df3 = multiple_condition_filter(filter_by_mkt_value(('300M',0)),
                                  filter_by_entry_cost('10K'),
                                  filter_by_over_ma_60())
        """

        # self.df = self.filter_by_status(1)
        for d in df_hub:
            self.df = self.merge_everything(self.df, d)
        return self.df

    @staticmethod
    def get_the_symlist(df: pd.DataFrame) -> list:
        """
        get the symlist by dataframe
        """
        return df.sym.tolist()


if __name__ == '__main__':
    flt = Filters()
