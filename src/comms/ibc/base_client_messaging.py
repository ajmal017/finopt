#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import json

from ib.ext.Contract import Contract
from misc2.helpers import ContractHelper, ExecutionFilterHelper, OrderHelper
from comms.ibgw.base_messaging import  BaseMessageListener
from misc2.observer import NotImplementedException
         

class GatewayCommandWrapper():

    def __init__(self, producer):
        self.producer = producer
  
    def reqOpenOrders(self):
        self.producer.send_message('reqOpenOrders', '')
    
    def reqIds(self):
        self.producer.send_message('reqIds', '')
    
    def reqNewsBulletins(self):
        logging.error('reqNewsBulletins: NOT IMPLEMENTED')
    
    def cancelNewsBulletins(self):
        logging.error('cancelNewsBulletins: NOT IMPLEMENTED')
    
    def setServerLogLevel(self):
        logging.error('setServerLogLevel: NOT IMPLEMENTED')
  
    def reqAutoOpenOrders(self):
        logging.error('reqAutoOpenOrders: NOT IMPLEMENTED')
    
    def reqAllOpenOrders(self):
        logging.error('reqAllOpenOrders: NOT IMPLEMENTED')
    
    def reqManagedAccts(self):
        logging.error('reqManagedAccts: NOT IMPLEMENTED')
    
    def requestFA(self):
        logging.error('requestFA: NOT IMPLEMENTED')
    
    def reqPositions(self):
        self.producer.send_message('reqPositions', '{}')
        
    def reqHistoricalData(self):
        logging.error('reqHistoricalData: NOT IMPLEMENTED')
        
    def reqAccountUpdates(self, subscribe, acct_code):
        self.producer.send_message('reqAccountUpdates', json.dumps({'subscribe': subscribe, 'acct_code': acct_code}))

    def reqExecutions(self, exec_filter=None):

        #self.producer.send_message('reqExecutions', json.dumps({'exec_filter': 'null'}))
        self.producer.send_message('reqExecutions', json.dumps({'exec_filter': ExecutionFilterHelper.object2kvstring(exec_filter) if exec_filter else 'null'}))

#     def reqMktData(self, contract):
#         self.producer.send_message('reqMktData', ContractHelper.object2kvstring(contract))
    def reqMktData(self, contract, snapshot=False):
        # send request to TWS gateway for market data
        # contract - typeof ib.ext.contract
        # snapshot - True/False default is false to receive continuous updates
        #
        self.producer.send_message('reqMktData', json.dumps({'contract': ContractHelper.object2kvstring(contract), 
                                                  'snapshot': snapshot}))        
        
    def reqAccountSummary(self, reqId, group, tags):
        self.producer.send_message('reqAccountSummary', self.producer.message_dumps([reqId, group, tags]))
    
    def placeOrder(self, id, contract, order):
        self.producer.send_message('placeOrder', 
                                   self.producer.message_dumps([id, ContractHelper.contract2kvstring(contract), OrderHelper.object2kvstring(order)]))
    
        


        
        
        
    
class AbstractGatewayListener(BaseMessageListener):
    
    def __init__(self, name):
        BaseMessageListener.__init__(self, name)
        
    
    def tickPrice(self, event, contract_key, field, price, canAutoExecute):  # tickerId, field, price, canAutoExecute):
        """ generated source for method tickPrice """
        raise NotImplementedException
   
    def tickSize(self, event, contract_key, tickerId, field, size):
        """ generated source for method tickSize """
        raise NotImplementedException
   
    def tickOptionComputation(self, event, message_value):  # tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        """ generated source for method tickOptionComputation """
        raise NotImplementedException
   
    def tickGeneric(self, event, message_value):  # tickerId, tickType, value):
        """ generated source for method tickGeneric """
        raise NotImplementedException
   
    def tickString(self, event, message_value):  # tickerId, tickType, value):
        """ generated source for method tickString """
        raise NotImplementedException
   
    def tickEFP(self, event, message_value):  # tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        """ generated source for method tickEFP """
        raise NotImplementedException
   
    def orderStatus(self, event, message_value):  # orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        """ generated source for method orderStatus """
        raise NotImplementedException
   
    def openOrder(self, event, message_value):  # orderId, contract, order, orderState):
        """ generated source for method openOrder """
        raise NotImplementedException
   
    def openOrderEnd(self, event, message_value):
        """ generated source for method openOrderEnd """
        raise NotImplementedException
   
    def update_portfolio_account(self, event, **message_value):
        '''
            this message wraps TWS events 'updateAccountValue', 'updatePortfolio',
            'updateAccountTime', 'accountDownloadEnd' and their parameters into a dict in 
            the message_value parameter
            
            the name of the tws event is stored in a key 'tws_event'
            
             val->[{u'currency': u'JPY', u'account': u'U8379890', u'value': u'0.0698037', u'tws_event': u'updateAccountValue', u'key': u'ExchangeRate'}]
 
            
        '''
        tws_event = message_value['tws_event']
        del(message_value['tws_event'])
        getattr(self, tws_event)(tws_event, **message_value)
        
   
#     def updateAccountValue(self, event, key, value, currency, account):  # key, value, currency, accountName):
#         """ generated source for method updateAccountValue """
#         raise NotImplementedException
# 
#     def updatePortfolio(self, event, contract_key, position, market_price, market_value, average_cost, unrealized_PNL, realized_PNL, account):
#         """ generated source for method updatePortfolio """
#         raise NotImplementedException
#    
#     def updateAccountTime(self, event, timestamp):
#         """ generated source for method updateAccountTime """
#         raise NotImplementedException
#    
#     def accountDownloadEnd(self, event, account):  # accountName):
#         """ generated source for method accountDownloadEnd """
#         raise NotImplementedException
   
    def nextValidId(self, event, message_value):  # orderId):
        """ generated source for method nextValidId """
        raise NotImplementedException
   
    def contractDetails(self, event, message_value):  # reqId, contractDetails):
        """ generated source for method contractDetails """
        raise NotImplementedException
   
    def bondContractDetails(self, event, message_value):  # reqId, contractDetails):
        """ generated source for method bondContractDetails """
        raise NotImplementedException
   
    def contractDetailsEnd(self, event, message_value):  # reqId):
        """ generated source for method contractDetailsEnd """
        raise NotImplementedException
   
    def execDetails(self, event, req_id, contract_key, order_id, side, price, avg_price, cum_qty, exec_id, account, exchange, order_ref, exec_time, end_batch): 
        """ generated source for method execDetails """
        raise NotImplementedException
    
   
#     def execDetailsEnd(self, event, req_id, end_batch):  # reqId):
#         """ generated source for method execDetailsEnd """
#         raise NotImplementedException
   
    def updateMktDepth(self, event, message_value):  # tickerId, position, operation, side, price, size):
        """ generated source for method updateMktDepth """
        raise NotImplementedException
   
    def updateMktDepthL2(self, event, message_value):  # tickerId, position, marketMaker, operation, side, price, size):
        """ generated source for method updateMktDepthL2 """
        raise NotImplementedException
   
    def updateNewsBulletin(self, event, message_value):  # msgId, msgType, message, origExchange):
        """ generated source for method updateNewsBulletin """
        raise NotImplementedException
   
    def managedAccounts(self, event, message_value):  # accountsList):
        """ generated source for method managedAccounts """
        raise NotImplementedException
   
    def receiveFA(self, event, message_value):  # faDataType, xml):
        """ generated source for method receiveFA """
        raise NotImplementedException
   
    def historicalData(self, event, message_value):  # reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        """ generated source for method historicalData """
        raise NotImplementedException
   
    def scannerParameters(self, event, message_value):  # xml):
        """ generated source for method scannerParameters """
        raise NotImplementedException
   
    def scannerData(self, event, message_value):  # reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        """ generated source for method scannerData """
        raise NotImplementedException
   
    def scannerDataEnd(self, event, message_value):  # reqId):
        """ generated source for method scannerDataEnd """
        raise NotImplementedException
   
    def realtimeBar(self, event, message_value):  # reqId, time, open, high, low, close, volume, wap, count):
        """ generated source for method realtimeBar """
        raise NotImplementedException
   
    def currentTime(self, event, message_value):  # time):
        """ generated source for method currentTime """
        raise NotImplementedException
   
    def fundamentalData(self, event, message_value):  # reqId, data):
        """ generated source for method fundamentalData """
        raise NotImplementedException
   
    def deltaNeutralValidation(self, event, message_value):  # reqId, underComp):
        """ generated source for method deltaNeutralValidation """
        raise NotImplementedException
   
    def tickSnapshotEnd(self, event, message_value):  # reqId):
        """ generated source for method tickSnapshotEnd """
        raise NotImplementedException
   
    def marketDataType(self, event, message_value):  # reqId, marketDataType):
        """ generated source for method marketDataType """
        raise NotImplementedException
   
    def commissionReport(self, event, message_value):  # commissionReport):
        """ generated source for method commissionReport """
        raise NotImplementedException
   
    def position(self, event, account, contract_key, pos, avg_cost, end_batch):
        """ generated source for method position """
        raise NotImplementedException
   
    def positionEnd(self, event): #, message_value):
        """ generated source for method positionEnd """
        raise NotImplementedException


        

   
    def accountSummary(self, event, message_value):  # reqId, account, tag, value, currency):
        """ generated source for method accountSummary """
        raise NotImplementedException
   
    def accountSummaryEnd(self, event, message_value):  # reqId):
        """ generated source for method accountSummaryEnd """
        raise NotImplementedException

     
    def error(self, id=None, errorCode=None, errorMsg=None):
        raise NotImplementedException
    
    def on_kb_reached_last_offset(self, event, message_value):  # event, items):
        logging.info("[%s] received on_kb_reached_last_offset content: [%s]" % (self.name, message_value))
        print "on_kb_reached_last_offset [%s] %s" % (self.name, message_value)
    

