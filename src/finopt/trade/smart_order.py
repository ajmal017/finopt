# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread
import ConfigParser
from ib.ext.Contract import Contract
from ib.ext.ExecutionFilter import ExecutionFilter
from ib.ext.Execution import Execution
from ib.ext.Order import Order
from ib.opt import ibConnection, message
from time import sleep
import time, datetime
import redis
import threading
from threading import Lock, Thread
from finopt.options_data import ContractHelper

            
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
    
    def __init__(self, right_type, rule):
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
        
class OrderManager(Thread):
    
        config = None
        tlock = None
        
        def __init__(self, config):
            super(OrderManager, self).__init__()
            self.config = config
            self.tlock = Lock()

        def on_ib_message(self, msg):
            print msg.typeName, msg
            if msg.typeName == 'openOrder':
                print ContractHelper.printContract(msg.contract)
            
        
        def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeId):
            pass
        
        
        def send_new_order(self):
            self.tlock.acquire()
            try:
                self.new_order_entrant_unsafe()
            finally:
                logging.debug('recal_port: completed recal. releasing lock...')
                self.tlock.release()  
            
        
        def new_order_entrant_unsafe(self):
            pass
        
        
        def reqExecutions(self):
            filt = ExecutionFilter()
            self.con.reqExecutions(0, filt)
        
        
        def run(self):
            host = config.get("market", "ib.gateway").strip('"').strip("'")
            port = int(config.get("market", "ib.port"))
            appid = int(config.get("market", "ib.appid.portfolio"))   
            
            self.con = ibConnection(host, port, appid)
            self.con.registerAll(self.on_ib_message)
            self.con.connect()
            while 1:
                self.con.reqAllOpenOrders()
                self.reqExecutions()
                sleep(2)


class OrderHelper():
    
    def make_option_order(self, action, orderID, tif, orderType):
        opt_order = Order()
        opt_order.m_orderId = orderID
        opt_order.m_clientId = 0
        opt_order.m_permid = 0
        opt_order.m_action = action
        opt_order.m_lmtPrice = 0
        opt_order.m_auxPrice = 0
        opt_order.m_tif = tif
        opt_order.m_transmit = False
        opt_order.m_orderType = orderType
        opt_order.m_totalQuantity = 1
        return opt_order        

if __name__ == '__main__':
           
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]    
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0:      
        raise ValueError, "Failed to open config file" 
    
    logconfig = eval(config.get("smart_order", "smart_order.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)       
    
    
    odm = OrderManager(config) 
    odm.start()
    
    
    
    
    
#     x = SmartOrderSelector()
#     print x
#     y = UnwindOrderSelector('c')
#     y.rule_IV()
#     print y
