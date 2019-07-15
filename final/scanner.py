# -*- coding: utf-8 -*-
"""
Created v1 on Sun Mar 11 09:30:49 2018
Created v2 on Tue Jul 31 13:15:22 2018
Created v3 on 20181030

@author: bransonngai
"""
# import argparse
import talib as ta

import BATrader as ba
from BATrader.final.product import Stock
from BATrader.final.data_handler import MemoryDict
from BATrader.algo.algo import last_signal_bool, chg, chgp
from BATrader.utils.toolbox import timeit
from BATrader.market.MarketReader import convert

# import backtrader as bt
# import backtrader.feeds as btfeeds
# import backtrader.indicators as btind

import pandas as pd

from queue import Queue
from threading import Thread

from BATrader.logger import Logger
from BATrader.final.filters import Filters

from collections import OrderedDict
from copy import copy

import inspect


def divided_and_show_pct(a, b):
    """
    Two period of data change in pecentage (without %)
    """
    return round((float(a) / float(b)) * 100, 2)


"""
# 劵商異動:持股量佔比日變動達2% 或 周變動達4% 或 月變動達10%
# 過去X period 中, 升跌Y點數或以上的日子有多少

# 中午收市後, 找一找30mins Break out的股票, 並pus
# 找30分鐘圖,調整完反彈的股票-e.g. 1108
# 現價對於上市價之比例

# 941 20140718 - 60/200 平均線乖離不超5%, 放量上攻, 連破2條平均線,open\<ma60\<ma200\<close
# 992 週線下破60天
# 當日夜晚8點, 至第二朝8點, 呢12個鐘發報既最新公告 (特別留意沒有炒作焦點一段時間的日子, 如大跌之後牛皮的日子)

## 每日報告:
大市上日PE

from HKEx import PE_Report
pe = PE_Report()
pe.output_excel()

## 星期報告:
#### 過去一個星期:
藍籌入面, 10大升幅, 整體升跌比例, 恒指升跌
主板入面, 10大升幅, 整體升跌比例
0~50億市值主板股票, 10大升跌
50~200億市值主板股票, 10大升跌
板塊升跌幅Heatmap
"""


# Vector Scanner
# http://www.bossqiao.com/02%20coding/

# ------ Scanner ------
class Scanner(object):
    """
    Created on Sun May 15 23:37:02 2016
    PriceServer v1 2016-05
    RTQ update Robot
    
    scan_list : the scan target
    logic_dic : key-value pair, contain a callback function and params
    scan_result : list
    
    實現方法1:
        Dataframe, if condition true then return 股票 sym
        multiple conditions, set 左佢, return all matched
        
    實現方法2:
        由頭掃到落尾
    last_week_up_x_pct
    2018-10 
    
        新增一個filter class, 當立即可以用symlist回答的, 就用filter 去實現, 非常快
        其次要做 dayback 計數的, 就用 scanner class.
        
        Scanner Class 的重新定義: 理應是Cerebro的設計, 像容器一般把所有東西放進去, 按
        個start就執行。 但scanner不同的地方是, 會有多於一個以上的condition, 而需要用到
        的data也不同, 故strategry class可以滿足經常改變的部份。
    
    2019-05
    
        因為 Strategy 終於成形, 發現只要將 s (Stock obj) 傳入一個獨立logic, 再return 
        True False, 已經可以成為一個Condiction. 而且可以重用。
        
        Scanner Class的設計絶妙, 彈性很大。
        
        如果要位移 ( e.g. Close[0] -> Close[1] ) 可以在 s obj 做手腳腳

    Usage:
        scanner = Scanner()
    
        scanner.addsymlist(ba.fr.get_main_gem())
        # scanner.addfilter('and', filter_by_over_ma_60())
        scanner.addfilter('and', flt.filter_by_amount(10000000.0))
        scanner.addfilter('and', flt.filter_by_over_ma(60))
        # scanner.addfilter('not', filter_ot_list())
        scanner.addcondition(HighVolatilityStock)
        #    scanner.addstrategy(TestStrategy)
    

    """

    def __init__(self, running_mode=0, cond_method= 0, recalc= False, dayback= None):

        self.run_mode = {0: 'SingleThread', 1: 'Threading'}.get(running_mode)
        self.cond_apply_method = {0: 'recursive', 1: 'independent'}.get(cond_method)  # Recursive is one-and-one

        self.strats = list()  # From Cerebro, addstrategy will add to here
        self.datas = list()

        self.scan_list = []  # the scan target container of each loop
        self.scan_list_groups = {}  # Sometimes we needs "Sector" -> { Sector : [symlist] }
        self.scan_result = []  # the scan result
        self.filters_and = []  # match
        self.filters_not = []  # not match
        self.condition_list = {}

        self.last_scan_result = {'result': OrderedDict()}  # the last scan result

        self.flt = Filters()
        
        # Memory dict
        MemoryDict.recalc = recalc
        MemoryDict.dayback = dayback
        self.memory_db = MemoryDict()

        # For threading
        self.queue = Queue()

        # logger
        self.logger = None

    @timeit
    def run(self):
        """
        Run like cerebro
        """
        # Logger should init when running, not the class init, otherwise
        # the log will be hold
        self.logger = Logger(logname=ba.fr.path_output + 'logger\\%s_scanner.log' % ba.dk.tradeday(),
                             logger='main').getlog()

        # Every time to run, copy the loaded_full_list
        # self.scan_list = copy(self.loaded_full_list)

        # Clear the last scan result
        self.last_scan_result = {'steps': OrderedDict()}  # the last scan result
        self.logger.debug('Running')
        self.logger.info("Scanner mode activated : {}".format(self.run_mode))
        if not self.scan_list:
            self.logger.info('Scan list is empty')
            return
        else:
            self.logger.info('Scan list size : %s' % len(self.scan_list))

        # ---------- Filters ------------
        if self.filters_and:
            for f in self.filters_and:
                self.scan_list = set(f).intersection(self.scan_list)
            self.logger.info('Filtering and logic : %s' % len(self.scan_list))
        if self.filters_not:
            for f in self.filters_not:
                self.scan_list = list(set(self.scan_list) - set(f))
            self.logger.info('Filtering not logic : %s' % len(self.scan_list))

        # ---------- Condition ----------
        if self.condition_list:
            
            # Single Thread
            if self.run_mode == 'SingleThread':            
                for name, func_tuple in list(self.condition_list.items()):
                    
                    self.logger.info('Scanning cond: {} with {} stocks.'.format(name, self.scan_list()))
                    
                    # condition
                    func        = func_tuple[0]
                    params_dict = func_tuple[1]
    
                    # loop tracker
                    steps_in_order_dic = self.last_scan_result['steps'][name] = {}  # last dic
                    true_hit_list = []

                    self.scan_list.sort(key= int)
                    for sym in self.scan_list:
                        self.logger.debug('Scanning {}'.format(sym))
                        try:
                            s = self.memory_db[sym]
                            if func(s, **params_dict):
                                true_hit_list.append(sym)
                                steps_in_order_dic.update({sym: True})
                                if name in s.scanner_meta_data:
                                    # If condition carry meta data, update to the key
                                    steps_in_order_dic.update({sym: s.scanner_meta_data[name]})
                        except Exception as e:
                            self.logger.info('Scanner Error : %s, %s' % (sym, str(e)))
                            pass
                        
                    if self.cond_apply_method == 'recursive':
                        # 使用上一層結果
                        self.scan_list = true_hit_list

                # Last scan result
                self.last_scan_result['result'] = true_hit_list

            # Multi Thread
            elif self.run_mode == 'Threading':

                # First put symlist in queue
                self.scan_list.sort(key= int)
                for sym in self.scan_list:
                    self.queue.put(sym)

                def scan(func, params_dict):
                    while not self.queue.empty():
                        sym = self.queue.get()
                        self.logger.debug('Scanning {}'.format(sym))
                        s = self.memory_db[sym]
                        if func(s, **params_dict):
                            true_hit_list.append(sym)
                            steps_in_order_dic.update({sym: True})
                            if name in s.scanner_meta_data:
                                steps_in_order_dic.update({sym: s.scanner_meta_data[name]})

                for name, func_tuple in list(self.condition_list.items()):

                    self.logger.info('Scanning cond: {} with {} stocks.'.format(name, self.queue.qsize()))
                    
                    # condition
                    func = func_tuple[0]
                    params_dict = func_tuple[1]

                    # loop tracker
                    steps_in_order_dic = self.last_scan_result['steps'][name] = {}  # last dic
                    true_hit_list = []

#                    t1 = Thread(target= scan, name='T1', args=(func, params_dict))
#                    t2 = Thread(target= scan, name='T2', args=(func, params_dict))
#                    t3 = Thread(target= scan, name='T3', args=(func, params_dict))
#                    t4 = Thread(target= scan, name='T4', args=(func, params_dict))
#    
#                    t1.start()
#                    t2.start()
#                    t3.start()
#                    t4.start()
#    
#                    t1.join()
#                    t2.join()
#                    t3.join()
#                    t4.join()
                    
                    NUM_THREADS = 4
                    threads = [Thread(target= scan, args=(func, params_dict)) for i in range(NUM_THREADS)]  # Create 2 threads
            
                    list([th.start() for th in threads])  # Make all threads start their activities
                    list([th.join() for th in threads])  # block until all threads are terminated

                    if self.cond_apply_method == 'recursive':
                        for sym in true_hit_list:
                            self.queue.put(sym)
                    else:
                        # Put scan list back to queue
                        self.scan_list.sort(key= int)
                        for sym in self.scan_list:
                            self.queue.put(sym)

                # Last scan result
                self.last_scan_result['result'] = true_hit_list
                                              

        # ---------- Finish ----------
        # Write to TSCI, so we can view the scan result in tsci
        if self.cond_apply_method == 'recursive':
            ba.sm.tsci_write_symlist_to_file('scanner_result', self.scan_list)
            self.logger.info('>>> Scan complete. Result: %s' % len(self.scan_list))
        else:
            self.logger.info('>>> Scan complete.')
        return self.last_scan_result


    @timeit
    def run_groups_scan(self):
        result = {}
        if self.scan_list_groups:
            for gp_name, gp_symlist in self.scan_list_groups.items():
                self.setsymlist(gp_symlist)
                result[gp_name] = self.run()
        self.last_scan_result = result
        return result             
                
        

    def result(self):
        # or simply the list(list(self.last_scan_result.items())[-1][1].keys())
        return self.scan_list

    def setsymlist(self, symlist: list):
        self.scan_list = symlist
        self.loaded_full_list = symlist

    def addsymlist(self, symlist: list):

        self.scan_list = list(set(self.scan_list + symlist))
        self.loaded_full_list = copy(self.scan_list)


    def addgroups(self, symlist_groups: dict):
        self.scan_list_groups = symlist_groups

    def addfilter(self, and_or_not, df):
        """
        'and' & df
        'not' & df
        """
        if and_or_not == 'and':
            self.filters_and.append(self.flt.get_the_symlist(df))
        elif and_or_not == 'not':
            self.filters_not.append(self.flt.get_the_symlist(df))

    def addcondition(self, condition, **kwargs):
        self.condition_list[condition.__name__] = (condition, kwargs)
        # self.condition_list.append(s: MemoryDict)

    def make(self, symlist):
        for s in symlist:
            yield s

    def make_queue(self, symlist):

        for sym in symlist:
            queue.put(sym)

        return queue


    def start_scan_hub(self):
        print("Scanner mode activated.")

        for i, scanner in enumerate(self.scanner_list):
            if i == 0:
                self.scan_symlist = self.symlist
            print('Start Scanning the %s criteria in %s stocks: %s' % (i, len(self.scan_symlist), scanner.name))

            scanner.symlist = self.scan_symlist
            result = scanner.start_scan()

            print('There is %s in result' % len(result))
            print()

            self.scan_symlist = result

        self.result = self.scan_symlist

        print('Here is the result: ', len(self.result))

    def today_have_signal(self, algo, symlist='', dayback=0):
        if not symlist:
            symlist = self.fr.getCleanlist()
        print(symlist)
        sig_list = []
        print('Scanning signal...')
        for sym in symlist:
            # print sym
            d = algo(self.tc.get_bar_ex_eod_db(sym))
            # print self.tradeday
            try:
                bool_ = d['Signal'].ix[self.tradeday]
            except KeyError:
                continue

            if bool_:
                sig_list.append(sym)
        print('Done.')
        return sig_list


# ------ Auxiliary related ------
def auxiliary_ground_rule(s: MemoryDict) -> bool:
    """
        9日平均成交額要大於3百萬
        成交量比前日大9倍
        股價必須大於2毫
        股價必須在 ma200 之上
    Args:
        s ():

    Returns:

    """
    daybar = s.daybar[-30:]

    total_turn_day = 9
    if len(daybar) < 9:
        total_turn_day = len(daybar)
    nine_day_turnover = ta.EMA(daybar.Turnover.values, total_turn_day)

    cond1 = nine_day_turnover[-1] >= 3000000  # 9日平均成交額要大於3百萬 或 (IPO) 平均成交大於3百萬
    cond2 = (daybar.Vol.iloc[-1] / daybar.Vol.iloc[-2]) > 9  # 成交量比前日大9倍
    cond3 = daybar.Close.iloc[-1] >= 0.20  # 股價必須大於2毫
    # cond4 = # 股價必須在 ma200 之上

    if (cond1 | cond2) & cond3:
        return True
    return False


def filter_production(s: MemoryDict, target_chgp: float) -> bool:
    """
    升幅％　大於　某個數值
    """
    if s.daybar_chgp > target_chgp:
        return True
    return False


# ------ Momentum related ------
def price_up_vol_up(s: MemoryDict, in_row=1):
    return last_signal_bool(ba.algo.Signal_p_up_vol_up(s.daybar, in_row))


def rainbow(s: MemoryDict, in_row=1):
    return last_signal_bool(ba.algo.Signal_MAHL_x_day_up(s.daybar, in_row))


def moving_average_condition(s: MemoryDict):
    """
    快線 高於 慢線
    """

    def __init__(self, *args):
        self.ma_period = args

    def data(self, sym):
        return ba.tc.get_bar_ex_eod_db(sym)

    def logic(self, sym):
        print('Moving Average (fast > slow) :', sym)
        ma_fast_p = self.ma_period[0][0]['fast_period']
        ma_slow_p = self.ma_period[0][0]['slow_period']

        ba.algo.set_data(self.data(sym))
        ba.algo.cmd_ema(ma_fast_p)
        ba.algo.cmd_ema(ma_slow_p)

        f = ba.algo.data['ema%s' % ma_fast_p].iloc[-1]
        s = ba.algo.data['ema%s' % ma_slow_p].iloc[-1]

        if f > s:
            result.append((sym, f, s))
            return True


def moving_average_golden_cross_condition(s: MemoryDict):
    """
    黃金交叉
    """

    def __init__(self, *args):
        self.ma_period = args

    def data(self, sym):
        return ba.tc.get_bar_ex_eod_db(sym)

    def logic(self, sym):
        print('Moving Average Golden Cross :', sym)
        ma_fast_p = self.ma_period[0][0]['fast_period']
        ma_slow_p = self.ma_period[0][0]['slow_period']

        ba.algo.set_data(self.data(sym))
        ba.algo.cmd_ema(ma_fast_p)
        ba.algo.cmd_ema(ma_slow_p)

        f0 = ba.algo.data['ema%s' % ma_fast_p].iloc[-1]
        s0 = ba.algo.data['ema%s' % ma_slow_p].iloc[-1]

        f1 = ba.algo.data['ema%s' % ma_fast_p].iloc[-2]
        s1 = ba.algo.data['ema%s' % ma_slow_p].iloc[-2]

        if f0 > s0 and f1 < s1:
            result.append((sym, f0, s0))
            return True


def moving_average_death_cross_condition(s: MemoryDict):
    """
    死亡交叉
    """

    def __init__(self, *args):
        self.ma_period = args

    def data(self, sym):
        return ba.tc.get_bar_ex_eod_db(sym)

    def logic(self, sym):
        print('Moving Average Death Cross :', sym)
        ma_fast_p = self.ma_period[0][0]['fast_period']
        ma_slow_p = self.ma_period[0][0]['slow_period']

        ba.algo.set_data(self.data(sym))
        ba.algo.cmd_ema(ma_fast_p)
        ba.algo.cmd_ema(ma_slow_p)

        f0 = ba.algo.data['ema%s' % ma_fast_p].iloc[-1]
        s0 = ba.algo.data['ema%s' % ma_slow_p].iloc[-1]

        f1 = ba.algo.data['ema%s' % ma_fast_p].iloc[-2]
        s1 = ba.algo.data['ema%s' % ma_slow_p].iloc[-2]

        if f0 < s0 and f1 > s1:
            result.append((sym, f0, s0))
            return True


def triple_ma_blazer(s: MemoryDict, *args) -> bool:
    """
    兩 set ma 交叉
    """
    g1 = (10, 20, 50)
    match = False
    d = ba.typical_ema_groups(s.daybar)
    if d['ema10'].iloc[-1] > d['ema10'].iloc[-2] > d['ema10'].iloc[-3]:
        if d['ema20'].iloc[-1] > d['ema20'].iloc[-2] > d['ema20'].iloc[-3]:
            if d['ema50'].iloc[-1] > d['ema50'].iloc[-2] > d['ema50'].iloc[-3]:
                match = True
    return match


# ------ Demand related ------
def sudden_vol_blowing(s: MemoryDict):
    """
    # 今日放量上升 (無消息, 無故大升)
    今日升 加上 放量倍數
    但是成交額都要跟得上, 起碼要1, 2千萬
    """

    d = s.daybar
    ratio = d.Vol.iat[-1] / d.Vol.iat[-2]
    match = False
    if ratio > self.times:
        try:
            turnover = ba.etnet.get_by_sym_sql(sym, exact_date=ba.dk.tradeday())['Turnover'].values[0]
            if turnover >= self.turnover:
                result.append((sym, ratio))
                match = True
        except:
            pass
    return match


def high_volatility_stock(s: MemoryDict):
    """
    ATR 對比 Close 大於 0.05 及
    ATR 對比 每格跳動 大於 30
    """

    match = False

    ba.algo.set_data(s.daybar)
    ba.algo.cmd_atr(10)
    d = ba.algo.data
    d['p'] = d.ATR / d.Close

    from BATrader.market.MarketReader import pricespread

    if d.p.iloc[-1] >= 0.05:
        if d.ATR.iloc[-1] / pricespread(d.Close.iloc[-1]) > 30:
            s.scanner_meta_data[inspect.stack()[0][3]] = (d.ATR.iloc[-1],  # ATR
             d.ATR.iloc[-1] / pricespread(d.Close.iloc[-1]),  # ATR / tick
             d.p.iloc[-1])  # ATR / Close]
        match = True

    return match


def unknown_func_last(s: MemoryDict):
    # Sector ond day momentum index

    sector = ba.etnet.etnet_concept_dic['內房股']
    # house_list = sm.fr.which_group(sector)

    etnet.get_by_sym_sql('1313', col_lst=['PE'])
    dic = etnet.etnet_concept_dic

    for sector, symlist in list(dic.items()):
        print(sector)

    #        path = self.path_input + 'sector.txt'
    #        with open(path,'a') as f:
    #            f.write(msg  + '\n')

    # 1 loops, best of 3: 37 s per loop
    ignore_sector = ['農業股']

    dayback = 1

    result_dic = {}
    df_dic = {}
    single_stock_df = pd.DataFrame()
    for chi_name, eng_name in list(etnet.etnet_concept_dic.items()):
        # print eng_name
        group = fr.which_group(eng_name)
        print(chi_name, eng_name, group)

        # Lv2 direct 做法
        #     d = rtq.get_aa_bunch_df(group, detail='lv2', col = ['Close','pClose'])
        #     d.Close = d.Close.astype('float')
        #     d.pClose = d.pClose.astype('float')

        # Lv1 + DB 做法
        d = rtq.get_aa_bunch_df(group, detail='lv1', col=['Close'])
        t = tc.get_bar_ex_eod_db(group, date=dk.tradeday(dayback), col=['Close'])
        t = t.reset_index().set_index('sym')[['Close']]
        t.columns = ['pClose']
        d = pd.concat([d, t], axis=1, sort=False)
        d.Close = d.Close.astype('float')
        d = d.dropna()
        df_dic[eng_name] = d

        d['SignleStockChgp'] = d.Close / d.pClose
        single_stock_df = single_stock_df.append(d)
        result_dic[chi_name] = d.Close.sum() / d.pClose.sum()

    r = {}
    for group, value in list(result_dic.items()):
        r[group] = (value - 1) * 100

    top20 = single_stock_df.sort_values('SignleStockChgp', ascending=False)[:20]

    df = pd.DataFrame(list(r.items()), columns=['Group', 'Value'])
    df.set_index('Group')
    top20_sector = pd.concat(
        [df.sort_values('Value', ascending=False)[:10], df.sort_values('Value', ascending=False)[-10:]], sort=False)

    print(top20)
    print(top20_sector)

    single_stock_df.sort_values('SignleStockChgp', ascending=False)[:20]

    for chi_name, eng_name in list(etnet.etnet_concept_dic.items()):
        # print eng_name
        group = fr.which_group(eng_name)
        print(eng_name, group)
        d = rtq.get_aa_bunch_df(group, detail='lv1', col=['Close'])
        t = tc.get_bar_ex_eod_db(group, date='20170524', col='Close')
        t = t.reset_index()
        t = t.set_index('sym')[['Close']]
        t.columns = ['pClose']
    d = pd.concat([d, t], axis=1, sort=False)

    sym = 2018

    from etnet import Etnet
    etnet = Etnet()
    etnet.get_tick(str(sym))[-20:]


def tick_is_over_5day_average(s: MemoryDict):
    def logic(self, sym):
        print('Tick is over 5days Average :', sym)
        df = ba.etnet.get_by_sym_sql(sym, col_lst=['tick'])

        import talib as ta
        df['ma5'] = ta.EMA(df['tick'].astype('float').values, 5)

        if df.ma5.iloc[-1] > df.ma5.iloc[-2]:
            return True


def inactive_and_thin(s: MemoryDict):
    """
    if last 5 days average trading amount is less than 1m, eliminate
    col name : inactive_or_thin
    """
    s.load_details(col_lst = ['tick','Turnover'], dayback= 11)
    data = s.details
    win = 5

    data['AvgT'] = data.Turnover.apply(lambda x: convert(x))
    data['AvgTick'] = data['tick'].apply(lambda x: convert(x)).rolling(window= win).mean()

    # if average turnover less than 1M and average Tick is less than 100 , kick out
    if data.AvgT[-1] <= 1000000 or data.AvgTick[-1] <= 100:
        return True
    return False


def low_liquidity(s: MemoryDict):
    """
    一隻股票在最新一天入面, 有超過5分鐘都冇成交, 代表liqudity低下, 或可剔除
    """ 
    s.load_min_bar('5') # It's simple mode
    lastday = s.data5min.reset_index()["Datetime"].map(lambda t: t.date().strftime('%Y%m%d')).unique()[-1]
    # ba.algo.make_min_bar(s.data1min.last('1D'), '5Min')
    
    # 壓縮成五分鐘燭 resample to 5 mins, 去掉唔要的時段
    data5m = s.data5min[lastday]

    # morning_session = data5m.between_time(start_time='09:30:00', end_time='11:59:00', include_end=True)
    # afternoon_session = data5m.between_time(start_time='13:00:00', end_time='15:59:00', include_end=True)
    # data5m = morning_session.append(afternoon_session)

    # 超過5分鐘都冇成交, 代表liqudity低下, 或可剔除
    # Adding half day filter : afternoon bar equal all zero bar
    #if len(afternoon_session) == len(afternoon_session.loc[afternoon_session.Vol == 0.0]):
        #if len(morning_session.loc[morning_session.Vol == 0.0]) > 2:
            #return True

    if len(data5m.loc[data5m.Vol == 0.0]) > 2:
        return True
    return False


# ------ Intraday related ------
def five_minutes_three_in_row(s: MemoryDict) -> bool:
    """
    Signal_3_in_row
    Args:
        s ():

    Returns:

    """
    if int(s.data1min.index[-1].strftime('%M')) % 5 != 0:
        return False
    
    s.load_min_bar('5')
    
    if last_signal_bool(ba.algo.Signal_3_in_row(s.data5min)):
        return True

    return False


def thirty_minues_blow_bbands(s: MemoryDict) -> bool:

    if int(s.data1min.index[-1].strftime('%M')) % 30 != 0:
        return False
    
    #ba.algo.set_data(ba.algo.make_min_bar(s.data1min, '30Min'))
    s.load_min_bar('30')
    if last_signal_bool(ba.algo.Signal_Blow_BBands(s.data30min)):
        return True

    return False


# ------ Swing Trade related ------
def is_today_t3b(s: MemoryDict):
    """
    # T3B2
    ba.algo.Signal_T3B2()
    generate_algo_msg('T3B2', ba.algo.data['Signal_T3B2'])
    """
    return last_signal_bool(ba.algo.Signal_T3B2(s.daybar))


def is_today_break_atr(s: MemoryDict):
    """
    # BreakATR
    ba.algo.Signal_BreakATR()
    generate_algo_msg('BreakATR', ba.algo.data['Signal_BreakATR'])

    """
    return last_signal_bool(ba.algo.Signal_BreakATR(s.daybar))


def is_today_over_ma_60_200(s: MemoryDict):
    """
    # >ma60/200
    ba.algo.Signal_Penetrate_ma60_ma200()
    generate_algo_msg('>ma60/200', ba.algo.data['Signal_penetrate_ma60_ma200'])
    """
    return last_signal_bool(ba.algo.Signal_Penetrate_ma60_ma200(s.daybar))


def is_today_break_bb_daybar(s: MemoryDict):
    """
    # breakBB_daybar
    ba.algo.Signal_Blow_BBands()
    generate_algo_msg('blowBB_daybar', ba.algo.data['Signal_Blow_BBands'])
    """
    return last_signal_bool(ba.algo.Signal_Blow_BBands(s.daybar))


# ------ Week related ------
def last_week_down_x_pct(s: MemoryDict, pct):
    """
    一個星期完結, 跌到 X % 以上
    燭身不能是長下影線
    """
    d = ba.algo.make_week_bar(s.daybar)
    d = chgp(d)

    if d['Chgp'].iloc[-1] < -pct:
        s.scanner_meta_data[inspect.stack()[0][3]] = d['Chgp'].iloc[-1]
        return True
    return False


def last_week_up_x_pct(s: MemoryDict, pct):
    """
    一個星期完結, 升到 X % 以上
    燭身不能是長上影線
    """
    d = ba.algo.make_week_bar(s.daybar)
    d = chgp(d)
    
    from BATrader.algo.algo import candle_wick
    
    d = candle_wick(d)

    if d['Chgp'].iloc[-1] > pct:
        if d.Wick.iloc[-1] != 1:
            s.scanner_meta_data[inspect.stack()[0][3]] = d['Chgp'].iloc[-1]
            return True
    return False


def scan_stock_relative_strong_weak():
    """
    # 大市走資比例, 如大市
    # 穿平均線比例 ( 10 , 20 , 60)
    """
    df[df.pClose > df.ma60]


# ------ Fundamental related ------
def pe_is_below(s: MemoryDict, pe=25):
    s.get_pe(s.sym)
    match = False
    if s.PE < pe:
        match = True
    return match


def ccass_trade_reach_certain_level_of_total_shares_condition(s: MemoryDict):
    def data(self, sym):
        day = ba.dk.tradeday()
        print(day)
        dic = ba.ccass.CCASS_flower_paper(sym, day)
        vol = ba.tc.get_bar_ex_eod_db(sym).loc[day]['Vol']  # e.g. 1853028.0
        dic.update({'vol': vol})
        return dic

    def logic(self, sym):
        print('CCASS condition for :', sym)
        d = self.data(sym)

        cond1 = d['top10%'] > 80  # 集中度大於80
        cond2 = divided_and_show_pct(d['total_in_ccass'], d['total_issued_shares']) < 30  # 在CCASS流通的股數非常少
        cond3 = divided_and_show_pct(d['vol'], d['total_in_ccass']) > 3  # 當日交易股數非常多

        if cond1 or cond2 or cond3:
            result.append((sym, d['top10%'],
                           divided_and_show_pct(d['total_in_ccass'], d['total_issued_shares']),
                           divided_and_show_pct(d['vol'], d['total_in_ccass'])))
            return True


def is_today_hkexnews_has_news(s: MemoryDict):
    s.load_hkexnews()


# ------ MMI related ------
def typical_mmi(s: MemoryDict):
    """
    (成交金融必須大於5百萬) 和 (Vol大於pVol 5倍) 和 ( A盤大於昨日A盤1.2倍 / 主動買入大於60 )
    ( A >= 55 和 A大過昨日 ) 和 (量 佔 昨天量比大於0.7) 和 (額 佔昨天額比大於0.7) 和 ( 額大於500萬 ) 或
    (額 大於 昨天額 2倍) 和 (A大於60) 和 (額大於9百萬) 或
    (額 大於 昨天額 6倍) 和 (額連續三日上升)
    
    (量 額 A 都大過昨日) 或
    (( d.Vol > d.Vol.shift(1)) & (d.Amount > d.Amount.shift(1)) & (d.A > d.A.shift(1))) |\
    """
    s.load_mmi(dayback= 10)
    return last_signal_bool(ba.algo.Signal_mmi(s.mmi))


if __name__ == '__main__':
    flt = Filters()
    #    sym = '2331'
    #    md = MemoryDict()
    #    s = md[sym]
    #    match = typical_mmi(s)

    #    self.run_mode = {0: 'SingleThread', 1: 'Threading'}.get(running_mode)
    #    self.cond_apply_method = {0: 'recursive', 1: 'independent'}  # Recursive is one-and-one

    scanner = Scanner(1, 1, dayback=30)

    #    scanner.addgroups({'內銀股': ['939','1398','2628','3988']})
    #    scanner.addcondition(last_week_up_x_pct, pct=5)
    #    scanner.addcondition(last_week_down_x_pct, pct=5)
    #    scanner.run_groups_scan()

    scanner.addsymlist(ba.fr.get_main_gem())
#    scanner.addfilter('and', filter_by_over_ma_60())
#    scanner.addfilter('and', flt.filter_by_amount(10000000.0))
#    scanner.addfilter('and', flt.filter_by_over_ma(60))
#    scanner.addfilter('not', filter_ot_list())
    scanner.addcondition(inactive_and_thin)
    scanner.addcondition(low_liquidity)
    result = scanner.run()

