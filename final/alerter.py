# -*- coding: utf-8 -*-
"""
Created on Mon Sep 22 12:07:03 2014

@author: Branson
"""
import sys
import BATrader as ba
from BATrader.final.action import MetaAction
from time import sleep
from threading import Thread
from BATrader.market.push import pushover_PriceAlert


class Alerter(MetaAction):
    """
    New RT alert class 2016-08
 
    Link to external, we call the "run" function, pass in the price dict,
    if we want to run independently, just run "start" function, if controls
    the update frequency, and call "run" itselfs. (close-end black box)
    
    Actually we need to build a j-notebook and update the Alerts list conveniently.
    Alerter should have some function to control the .txt, the one and only one
    file to keep the stocks be alerted. The j-notebook UI should just call those API
    
    2018-03 Adding json support in GZ

    2018-03 Usage:
        alerter.sym  =  6030
        alerter.value=  19
        alerter.add_alert_price_over()

    2019-07 Rewrite:
        Can standalone running (by config period)
        Can listen server price push (very low cost)
    """

    def __init__(self):

        super(Alerter, self).__init__('debug')

        self.refresh_time = 60  # for standalone mode
        self.alert_type_lst = ['P>=', 'P<=', 'Up$>=', 'Down$>=', 'Up%>=', 'Down%>=', 'Amount>=', 'Vol>=']
        
        self.connect_price_server()

    @staticmethod
    def read_price_alert_txt():
        """
        read the txt file and use eval to change text to dic object
        """
        f = open('..\config\price_alert.txt', 'r').read()
        return eval(f)

    @staticmethod
    def write_price_alert_txt(dic_obj):
        """
        write the dic object back to the txt file
        """
        f = open('..\config\price_alert.txt', 'w')
        f.write(str(dic_obj))
        f.close()

    def _add_sym_to_alert(self, sym, alert_type, value):
        """
        specify the sym and alert_type, and write to txt file
        """
        dic = self.read_price_alert_txt()
        if alert_type in self.alert_type_lst:
            dic[alert_type][sym] = value
            self.write_price_alert_txt(dic)
        else:
            print('Alert type not corret, please check')

    def _delete_sym_from_alert(self, sym, alert_type):
        """
        delete the sym from alert
        """
        dic = self.read_price_alert_txt()
        if alert_type in self.alert_type_lst:
            dic[alert_type].pop(sym)
            self.write_price_alert_txt(dic)
        else:
            print('Alert type not correct, please check')

    def return_symlist_in_price_alert_txt(self):
        """
        Return symlist exists in the config
        """
        dic = self.read_price_alert_txt()
        alert_sym_list = []
        for key, value in list(dic.items()):
            for key, value in list(value.items()):
                alert_sym_list.append(key)
        return alert_sym_list

    # ==============================================================================
    # ------ Alret add or delete ------     
    # ==============================================================================
    # P>=
    def add_alert_price_over(self, sym, value):
        self._add_sym_to_alert(str(sym), 'P>=', value)

    def delete_alert_price_over(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'P>=')

    # P<=
    def add_alert_price_below(self, sym, value):
        self._add_sym_to_alert(str(sym), 'P<=', value)

    def delete_alert_price_below(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'P<=')

    # Chg>=
    def add_alert_chg_over(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Up$>=', value)

    def delete_alert_chg_over(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Up$>=')

    # Chg<=
    def add_alert_chg_below(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Down$>=', value)

    def delete_alert_chg_below(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Down$>=')

    # Chgp>=
    def add_alert_chgp_over(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Up%>=', value)

    def delete_alert_chgp_over(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Up%>=')

    # Chgp<=
    def add_alert_chgp_below(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Down%>=', value)

    def delete_alert_chgp_below(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Down%>=')

    # Chgp>=
    def add_alert_amount_over(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Amount>=', value)

    def delete_alert_amount_over(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Amount>=')

    # Vol>=
    def add_alert_vol_over(self, sym, value):
        self._add_sym_to_alert(str(sym), 'Vol>=', value)

    def delete_alert_vol_over(self, sym=''):
        self._delete_sym_from_alert(str(sym), 'Vol>=')
        
    # =============================================================================
    # ------ Run ------
    # =============================================================================
    def run_standalone(self, price_dict={}):
        """
        If we pass in the price dict, we use it
        if dont, we connect and update
        """
        dic = self.read_price_alert_txt()
        
        keep_running = True
        while keep_running:

            if len(price_dict) == 0:
                alert_sym_list = self.return_symlist_in_price_alert_txt()
                price_dict = ba.tc.tc_rtq_bunch(alert_sym_list)
            else:
                break
    
            for alert_type, kvpair in list(dic.items()):
                for sym, value in list(kvpair.items()):
                    if alert_type == 'P>=':
                        if price_dict[sym]['close'] >= value:
                            pushover_PriceAlert('%s %s價格>=$%s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'P>=')
                    elif alert_type == 'P<=':
                        if price_dict[sym]['close'] <= value:
                            pushover_PriceAlert('%s %s價格<=$%s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'P<=')
                    elif alert_type == 'Up$>=':
                        if price_dict[sym]['chg'] >= value:
                            pushover_PriceAlert('%s %s上升>=$%s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Up$>=')
                    elif alert_type == 'Down$>=':
                        if price_dict[sym]['chg'] >= (float(value) * -1):
                            pushover_PriceAlert('%s %s下跌>=$%s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Down$>=')
                    elif alert_type == 'Up%>=':
                        if price_dict[sym]['chgp'] >= value:
                            pushover_PriceAlert('%s %s上升超過 %s%%' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Up%>=')
                    elif alert_type == 'Down%=':
                        if price_dict[sym]['chgp'] >= (float(value) * -1):
                            pushover_PriceAlert('%s %s下跌超過 %s%%' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Down%>=')
                    elif alert_type == 'Amount>=':
                        if price_dict[sym]['amount'] >= value:
                            pushover_PriceAlert('%s %s成交額超過 $ %s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Amount>=')
                    elif alert_type == 'Vol>=':
                        if price_dict[sym]['vol'] >= value:
                            pushover_PriceAlert('%s %s成交量超過 %s' % (sym, price_dict[sym]['name'], value))
                            self._delete_sym_from_alert(sym, 'Vol>=')
                            
                            
            sleep(self.refresh_time)




if __name__ == "__main__":
    alerter = Alerter()
