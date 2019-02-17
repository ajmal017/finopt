#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import json
import traceback
from time import strftime 
from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper, HelperFunctions
from comms.ibgw.base_messaging import BaseMessageListener

from ib.ext.Contract import Contract
from ib.ext.ExecutionFilter import ExecutionFilter
from ib.ext.Execution import Execution
from ib.ext.OrderState import OrderState
from ib.ext.Order import Order


class ClientRequestHandler(BaseMessageListener):
    
    def __init__(self, name, tws_gateway):
        BaseMessageListener.__init__(self, name)
        self.tws_connect = tws_gateway.tws_connection
            

    
    
    def reqAccountUpdates(self, event, subscribe, acct_code):
        logging.info('ClientRequestHandler - reqAccountUpdates value=%s' % vars())
        self.tws_connect.reqAccountUpdates(subscribe, HelperFunctions.utf2asc(acct_code))
    
    def reqAccountSummary(self, event, value):
        logging.info('ClientRequestHandler - reqAccountSummary value=%s' % value)
        #old stuff
#         vals = map(lambda x: x.encode('ascii') if isinstance(x, unicode) else x, json.loads(value))
#         self.tws_connect.reqAccountSummary(vals[0], vals[1], vals[2])
        
    def reqOpenOrders(self, event, value=None):
        self.tws_connect.reqOpenOrders()
    
    def reqPositions(self, event, value=None):
        self.tws_connect.reqPositions()
        
        
    def reqExecutions(self, event, exec_filter=None):
        
        logging.info('ClientRequestHandler - reqExecutions exec_filter string=%s' % exec_filter)
        if exec_filter == 'null':
            ef = ExecutionFilter()
        else:
            ef = ExecutionFilterHelper.kvstring2object(exec_filter, ExecutionFilter)
        self.tws_connect.reqExecutions(0, ef)
    
    
    def reqIds(self, event, value=None):
        self.tws_connect.reqIds(1)
    
    
    def reqNewsBulletins(self, event):
        self.tws_connect.reqNewsBulletins(1)
    
    
    def cancelNewsBulletins(self, event ):
        self.tws_connect.cancelNewsBulletins()
    
    
    def setServerLogLevel(self, event):
        self.tws_connect.setServerLogLevel(3)
    
    
    def reqAutoOpenOrders(self, event):
        self.tws_connect.reqAutoOpenOrders(1)
    
    
    def reqAllOpenOrders(self, event):
        self.tws_connect.reqAllOpenOrders()
    
    
    def reqManagedAccts(self, event):
        self.tws_connect.reqManagedAccts()
    
    
    def requestFA(self, event):
        self.tws_connect.requestFA(1)
    
    
    def reqMktData(self, event, contract, snapshot):
        logging.info('ClientRequestHandler received reqMktData request: %s' % contract)
#         try:
#             #self.contract_subscription_mgr.reqMktData(ContractHelper.kvstring2contract(sm_contract))
#             self.contract_subscription_mgr.reqMktData(ContractHelper.kvstring2object(contract, Contract))
#         except:
#             pass
    
    def reqHistoricalData(self, event):
        contract = Contract()
        contract.m_symbol = 'QQQQ'
        contract.m_secType = 'STK'
        contract.m_exchange = 'SMART'
        endtime = strftime('%Y%m%d %H:%M:%S')
        self.tws_connect.reqHistoricalData(
            tickerId=1,
            contract=contract,
            endDateTime=endtime,
            durationStr='1 D',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=0,
            formatDate=1)
    
    
    def placeOrder(self, event, value=None):
        logging.info('TWS_gateway - placeOrder value=%s' % value)
        try:
            vals = json.loads(value)
        except ValueError:
            logging.error('TWS_gateway - placeOrder Exception %s' % traceback.format_exc())
            return
        
    #        c = ContractHelper.kvstring2contract(vals[1])
        o = OrderHelper.kvstring2object(vals[2], Order)
        o.__dict__['transmit'] = self.ib_order_transmit
    #         print c.__dict__
    #         print o.__dict__
    #         print '---------------------'
    
           
        #self.connection.placeOrder(vals[0], ContractHelper.kvstring2contract(vals[1]), OrderHelper.kvstring2object(vals[2], Order))
        self.tws_connect.placeOrder(vals[0], ContractHelper.kvstring2object(vals[1], Contract), OrderHelper.kvstring2object(vals[2], Order))
    #        self.connection.placeOrder(orderId, contract, newOptOrder)
        

    
    
    def gw_req_subscriptions(self, event, value=None):
        logging.info('TWS_gateway - gw_req_subscriptions')
            
