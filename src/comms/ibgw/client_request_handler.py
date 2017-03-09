#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import json
import traceback
from time import strftime 
from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
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
            

    
    
    def reqAccountUpdates(self, value=None):
        logging.info('ClientRequestHandler - reqAccountUpdates value=%s' % value)
        self.tws_connect.reqAccountUpdates(1, '')
    
    def reqAccountSummary(self, value):
        logging.info('ClientRequestHandler - reqAccountSummary value=%s' % value)
        
        vals = map(lambda x: x.encode('ascii') if isinstance(x, unicode) else x, json.loads(value))
        self.tws_connect.reqAccountSummary(vals[0], vals[1], vals[2])
        
    def reqOpenOrders(self, value=None):
        self.tws_connect.reqOpenOrders()
    
    def reqPositions(self, value=None):
        self.tws_connect.reqPositions()
        
        
    def reqExecutions(self, value):
        try:
            filt = ExecutionFilter() if value == '' else ExecutionFilterHelper.kvstring2object(value, ExecutionFilter)
            self.tws_connect.reqExecutions(0, filt)
        except:
            logging.error(traceback.format_exc())
    
    
    def reqIds(self, value=None):
        self.tws_connect.reqIds(1)
    
    
    def reqNewsBulletins(self):
        self.tws_connect.reqNewsBulletins(1)
    
    
    def cancelNewsBulletins(self):
        self.tws_connect.cancelNewsBulletins()
    
    
    def setServerLogLevel(self):
        self.tws_connect.setServerLogLevel(3)
    
    
    def reqAutoOpenOrders(self):
        self.tws_connect.reqAutoOpenOrders(1)
    
    
    def reqAllOpenOrders(self):
        self.tws_connect.reqAllOpenOrders()
    
    
    def reqManagedAccts(self):
        self.tws_connect.reqManagedAccts()
    
    
    def requestFA(self):
        self.tws_connect.requestFA(1)
    
    
    def reqMktData(self, sm_contract):
        logging.info('ClientRequestHandler received reqMktData request: %s' % sm_contract)
        try:
            #self.contract_subscription_mgr.reqMktData(ContractHelper.kvstring2contract(sm_contract))
            self.contract_subscription_mgr.reqMktData(ContractHelper.kvstring2object(sm_contract, Contract))
        except:
            pass
    
    def reqHistoricalData(self):
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
    
    
    def placeOrder(self, value=None):
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
        

    
    
    """
       Client requests to TWS_gateway
    """
    def gw_req_subscriptions(self, value=None):
        
        #subm = map(lambda i: ContractHelper.contract2kvstring(self.contract_subscription_mgr.handle[i]), range(len(self.contract_subscription_mgr.handle)))
        #subm = map(lambda i: ContractHelper.object2kvstring(self.contract_subscription_mgr.handle[i]), range(len(self.contract_subscription_mgr.handle)))
        subm = map(lambda i: (i, ContractHelper.object2kvstring(self.contract_subscription_mgr.handle[i])), range(len(self.contract_subscription_mgr.handle)))
        
        print subm
        if subm:
            self.producer.send_message('gw_subscriptions', self.produce.message_dumps({'subscriptions': subm}))
            
            
