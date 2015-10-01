# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread
import ConfigParser
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message
from time import sleep
import time, datetime
import optcal
import opt_serve
import redis
from helper_func.py import dict2str, str2dict
from options_data import ContractHelper 
import portfolio_ex
            
# Tick Value      Description
# 5001            impl vol
# 5002            delta
# 5003            gamma
# 5004            theta
# 5005            vega
# 5005            premium
# IB tick types code reference
# https://www.interactivebrokers.com/en/software/api/api.htm
                    

class SmartOrderSelector(object):
    
    ORDER_SIDE_BUYBACK = 10000
    ORDER_SIDE_SELLOFF = 10100
    
    def __init__(self):
        pass
        
    def refresh_latest_pos(self):
        print 'refreshLatestPosition'
    

    
    def rule_IV(self, mode):
        
        pass
    
    def rule_spread(self):
        pass
    
    def rule_theta(self):
        pass
    
    def rule_month(self):
        pass
    
    def rule_dist_from_spot(self):
        pass
    
    def all_call_pos(self):
        pass
    
    def all_put_pos(self):
        pass
    
    
    def select_best_order(self):
        # return the best order from the portfolio to trade
        return Order()
    
    def __str__(self):
        return ''.join(('%s=%s')%(k,v) for k, v in self.__dict__.iteritems())

class UnwindOrderSelector(SmartOrderSelector):
    right_type = None
    
    def __init__(self, right_type. rule):
        self.right_type = right_type
        
    
    def pos_min_IV(self):
        Contracts = []
        
        return []
        
    def rule_IV(self, mode):
        print 'unwindorderselector:rule_iv'
        if mode == self.ORDER_SIDE_BUYBACK:
            # returns a list of the top three instrument with the 
            # lowest IV in ascending order
            # return self.pos_min_IV()
            
            pass
        elif mode == self.ORDER_SIDE_SELLOFF:
            # return self.pos_max_IV()
            pass

    
    def rule_spread(self, mode):
        print 'unwindorderselector:rule_iv'
        if mode == self.ORDER_SIDE_BUYBACK:
            # returns a list of the top three instrument with the 
            # lowest IV in ascending order
            # return self.pos_min_IV()
            
            pass
        elif mode == self.ORDER_SIDE_SELLOFF:
            # return self.pos_max_IV()
            pass
        
        
     
        

if __name__ == '__main__':
           
    logging.basicConfig(#filename = "log/opt.log", filemode = 'w', 
                        level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')      
    
    config = ConfigParser.ConfigParser()
    config.read("config/app.cfg")
    
    x = SmartOrderSelector()
    print x
    y = UnwindOrderSelector('c')
    y.rule_IV()
    print y
