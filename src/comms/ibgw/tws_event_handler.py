#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import strftime
import logging
import traceback
from ib.ext.EWrapper import EWrapper


        
class TWS_event_handler(EWrapper):

    TICKER_GAP = 1000
    producer = None
    
    def __init__(self, producer):        
        self.producer = producer
        
 
 
    def set_subscription_manager(self, subscription_manager):
        self.subscription_manger = subscription_manager
 
    def broadcast_event(self, message, mapping):

        try:
            dict = self.tick_process_message(message, mapping)     
            if message == 'gw_subscriptions' or message == 'gw_subscription_changed':   
                logging.info('TWS_event_handler: broadcast event: %s [%s]' % (dict['typeName'], dict))
            logging.info('broadcast_event %s' % dict)
            self.producer.send_message(message, self.producer.message_dumps(dict))    
        except:
            logging.error('broadcast_event: exception while encoding IB event to client:  [%s]' % message)
            logging.error(traceback.format_exc())
            
            
            

    
    def tick_process_message(self, message_name, items):
        

        t = items.copy()
        # if the tickerId is in the snapshot range
        # deduct the gap to derive the original tickerId
        # --- check logic in subscription manager
        try:
            
            if (t['tickerId']  >= TWS_event_handler.TICKER_GAP):
                #print 'tick_process_message************************ SNAPSHOT %d' % t['tickerId']
                t['tickerId'] = t['tickerId']  - TWS_event_handler.TICKER_GAP
                
        except (KeyError, ):
            pass          
            
        try:
            del(t['self'])
        except (KeyError, ):
            pass          
        

        for k,v in t.iteritems():
                #print k, v, type(v)
                #if type(v) in [Contract, Execution, ExecutionFilter, OrderState, Order, CommissionReport]:
            if 'ib.ext.' in str(type(v)):     
                t[k] = v.__dict__
            else:
                t[k] = v
        
               
        
        return t  
            
                
    
    def tickPrice(self, tickerId, field, price, canAutoExecute):
        logging.info('TWS_event_handler:tickPrice. %d<->%s' % (tickerId,self.subscription_manger.get_contract_by_id(tickerId) ))
        self.broadcast_event('tickPrice', {'contract_key': self.subscription_manger.get_contract_by_id(tickerId), 
                                          'field': field, 'price': price, 'canAutoExecute': canAutoExecute})
        #pass
    
    def tickSize(self, tickerId, field, size):
         logging.info('TWS_event_handler:tickSize. %d<->%s' % (tickerId,self.subscription_manger.get_contract_by_id(tickerId) ))
         self.broadcast_event('tickSize', {'contract_key': self.subscription_manger.get_contract_by_id(tickerId), 
                                            'field': field, 'size': size})
        #pass
    
    
    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        
        #self.broadcast_event('tickOptionComputation', self.tick_process_message(vars())) #vars())
        pass

    def tickGeneric(self, tickerId, tickType, value):
        #self.broadcast_event('tickGeneric', vars())
        pass 

    def tickString(self, tickerId, tickType, value):
        #self.broadcast_event('tickString', vars())
        pass 

    def tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        #self.broadcast_event('tickEFP', vars())
        pass

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeId):
        self.broadcast_event('orderStatus', vars())

    def openOrder(self, orderId, contract, order, state):
        self.broadcast_event('openOrder', vars())

    def openOrderEnd(self):
        self.broadcast_event('openOrderEnd', vars())

    def updateAccountValue(self, key, value, currency, accountName):
        self.broadcast_event('updateAccountValue', vars())

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        self.broadcast_event('updatePortfolio', vars())

    def updateAccountTime(self, timeStamp):
        self.broadcast_event('updateAccountTime', vars())

    def accountDownloadEnd(self, accountName):
        self.broadcast_event('accountDownloadEnd', vars())

    def nextValidId(self, orderId):
        self.broadcast_event('nextValidId', vars())

    def contractDetails(self, reqId, contractDetails):
        self.broadcast_event('contractDetails', vars())

    def contractDetailsEnd(self, reqId):
        self.broadcast_event('contractDetailsEnd', vars())

    def bondContractDetails(self, reqId, contractDetails):
        self.broadcast_event('bondContractDetails', vars())

    def execDetails(self, reqId, contract, execution):
        self.broadcast_event('execDetails', vars())

    def execDetailsEnd(self, reqId):
        self.broadcast_event('execDetailsEnd', vars())

    def connectionClosed(self):
        self.broadcast_event('connectionClosed', {})

    def error(self, id=None, errorCode=None, errorMsg=None):
        try:
            logging.error(self.tick_process_message('error', vars()))
            self.broadcast_event('error', {'id': id, 
                                           'errorCode': errorCode, 'errorMsg': errorMsg})

        except:
            pass

    def error_0(self, strvalue=None):
        logging.error(self.tick_process_message('error_0', vars()))
        self.broadcast_event('error_0', vars())

    def error_1(self, id=None, errorCode=None, errorMsg=None):
        logging.error(self.tick_process_message('error_1', vars()))        
        self.broadcast_event('error_1', vars())

    def updateMktDepth(self, tickerId, position, operation, side, price, size):
        self.broadcast_event('updateMktDepth', vars())

    def updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
        self.broadcast_event('updateMktDepthL2', vars())

    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        self.broadcast_event('updateNewsBulletin', vars())

    def managedAccounts(self, accountsList):
        logging.info(self.tick_process_message('managedAccounts', vars()))
        self.broadcast_event('managedAccounts', vars())

    def receiveFA(self, faDataType, xml):
        self.broadcast_event('receiveFA', vars())

    def historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        self.broadcast_event('historicalData', vars())

    def scannerParameters(self, xml):
        self.broadcast_event('scannerParameters', vars())

    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        self.broadcast_event('scannerData', vars())


    def commissionReport(self, commissionReport):
        self.broadcast_event('commissionReport', vars())


    def currentTime(self, time):
        self.broadcast_event('currentTime', vars())

    def deltaNeutralValidation(self, reqId, underComp):
        self.broadcast_event('deltaNeutralValidation', vars())


    def fundamentalData(self, reqId, data):
        self.broadcast_event('fundamentalData', vars())

    def marketDataType(self, reqId, marketDataType):
        self.broadcast_event('marketDataType', vars())


    def realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
        self.broadcast_event('realtimeBar', vars())

    def scannerDataEnd(self, reqId):
        self.broadcast_event('scannerDataEnd', vars())


    def tickSnapshotEnd(self, reqId):
        self.broadcast_event('tickSnapshotEnd', vars())


    def position(self, account, contract, pos, avgCost):
        #self.broadcast_event('position', vars())
        self.broadcast_event('position', vars())
        

    def positionEnd(self):
        self.broadcast_event('positionEnd', vars())

    def accountSummary(self, reqId, account, tag, value, currency):
        self.broadcast_event('accountSummary', vars())

    def accountSummaryEnd(self, reqId):
        self.broadcast_event('accountSummaryEnd', vars())
