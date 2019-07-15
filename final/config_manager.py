# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 16:35:38 2019

@author: Branson
"""
from BATrader.fileReader import read_ini_config



class ConfigManager_old(object):
    """
    The Config Manage 1.0
    """

    def __init__(self, config_name):
        self.config = {}
        self.config['strategy_data'] = {}
        self.config['backtest_data'] = {}
        try:
            with open(os.path.join(data_dir, "bsgroup.config.json")) as fp:
                self.config.update(json.load(fp))
        except IOError as error:
            pass
        for r in ['strategy_data']:
            for k in self.config[r]:
                self.config[r][k]['status'] = "stopped"
                self.config[r][k]['comment'] = ""
                send_dict("LocalStrategyStatus",
                          {"strategy": self.config[r][k]['strategy'],
                           "id": self.config[r][k]['id'],
                           "status": "stopped",
                           "comment": ""})

    def save(self):
        with open(os.path.join(data_dir, "bsgroup.config.json"), 'w') as fp:
            json.dump(self.config, fp, indent=4, sort_keys=True)

    def get(self, s):
        return self.config.get(s, None)

    def set(self, s, v, save=True):
        self.config[s] = v
        if save:
            self.save()

    def save_stratdata(self, root, kwargs, save=True):
        sid = kwargs['id']
        self.config[root][sid] = kwargs
        if save:
            self.save()

    def get_stratdata_by_strategy(self, root):
        d = {}
        for k, v in list(self.config[root].items()):
            if v['strategy'] not in d:
                d[v['strategy']] = []
            d[v['strategy']].append(v)
        for k in strategy.strategy_list():
            if k not in d:
                d[k] = []
        return d


class ConfigManager(object):

    def __init__(self, config_name, *addictional_config):
        self.config = read_ini_config(config_name)

        # session interval
        self.rapid_market_interval_s  = float(self.config['market_interval_s']['rapid'])
        self.normal_market_interval_s = float(self.config['market_interval_s']['normal'])
        self.refresh_interval_s       = float(self.config['market_interval_s']['refresh'])

        self.rapid_market_interval_f  = float(self.config['market_interval_f']['rapid'])
        self.normal_market_interval_f = float(self.config['market_interval_f']['normal'])
        self.refresh_interval_f       = float(self.config['market_interval_f']['refresh'])

        # zeromq port
        self.zeromq_port_pub   = self.config['zeromq_port']['pricer_pub_port']
        self.zeromq_port_stock   = self.config['zeromq_port']['pricer_stock_port']
        self.zeromq_port_futures = self.config['zeromq_port']['pricer_futures_port']

        # zeromq address
        self.address_pub = "tcp://127.0.0.1:%s" % self.zeromq_port_pub
        self.address_stock   = "tcp://127.0.0.1:%s" % self.zeromq_port_stock  # 5555
        self.address_futures = "tcp://127.0.0.1:%s" % self.zeromq_port_futures  # 7777
        self.address_ticker  = "tcp://127.0.0.1:%s" % self.config['zeromq_port']['ticker_subscribe_port']  # 5557

        # backtest
        self.backtest_push_time = float(self.config['backtest']['push_time'])
        self.backtest_push_time_s = float(self.config['backtest']['push_time_s'])
        self.backtest_push_time_f = float(self.config['backtest']['push_time_f'])
        
        if addictional_config:
            for name in addictional_config:
                self.addictional_config(name)


    def addictional_config(self, config_name):
        self.config.update(read_ini_config(config_name))
        
        # port
        self.push_pull_port = self.config['zeromq_port']['push_pull_port']
        self.req_rep_port = self.config['zeromq_port']['req_rep_port']
        self.trading_server_ip = self.config['server']['ip']