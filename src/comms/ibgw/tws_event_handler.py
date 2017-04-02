#!/usr/bin/env python
# -*- coding: utf-8 -*-
from misc2.helpers import ContractHelper
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
            dict = self.pre_process_message(message, mapping)     
            logging.info('broadcast_event %s:%s' % (message, dict))
            self.producer.send_message(message, self.producer.message_dumps(dict))    
        except:
            logging.error('broadcast_event: exception while encoding IB event to client:  [%s]' % message)
            logging.error(traceback.format_exc())
            
            
            

    
    def pre_process_message(self, message_name, items):

        #t = items.copy()
        t = items
       
            
#         try:
#             del(t['self'])
#         except (KeyError, ):
#             pass          
        if 'self' in t:
            del(t['self'])
            

        for k,v in t.iteritems():
                #print k, v, type(v)
                #if type(v) in [Contract, Execution, ExecutionFilter, OrderState, Order, CommissionReport]:
            if 'ib.ext.' in str(type(v)):     
                t[k] = v.__dict__
            elif 'exceptions.' in str(type(v)):
                t[k] = '%s:%s' % (str(type(v)), str(v))
            else:
                t[k] = v
        
               
        
        return t  
            
                
    
    def tickPrice(self, tickerId, field, price, canAutoExecute):
        logging.debug('TWS_event_handler:tickPrice. %d<->%s' % (tickerId,self.subscription_manger.get_contract_by_id(tickerId) ))
        self.broadcast_event('tickPrice', {'contract_key': self.subscription_manger.get_contract_by_id(tickerId), 
                                          'field': field, 'price': price, 'canAutoExecute': canAutoExecute})
        #pass
    
    def tickSize(self, tickerId, field, size):
         logging.debug('TWS_event_handler:tickSize. %d<->%s' % (tickerId,self.subscription_manger.get_contract_by_id(tickerId) ))
         self.broadcast_event('tickSize', {'contract_key': self.subscription_manger.get_contract_by_id(tickerId), 
                                            'field': field, 'size': size})
        #pass
    
    
    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        
        #self.broadcast_event('tickOptionComputation', self.pre_process_message(vars())) #vars())
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
        pass

    def openOrder(self, orderId, contract, order, state):
        pass

    def openOrderEnd(self):
        pass


    def update_portfolio_account(self, items):
        self.broadcast_event('update_portfolio_account', items)
        
    def updateAccountValue(self, key, value, currency, accountName):
        
        logging.info('TWS_event_handler:updateAccountValue. [%s]:%s' % (key.ljust(40), value))
        self.update_portfolio_account({'tws_event': 'updateAccountValue', 
                              'key': key, 'value': value, 'currency': currency, 'account':accountName})
#         self.broadcast_event('updateAccountValue', {'key': key, 
#                                           'value': value, 'currency': currency, 'account':accountName, 'batch_end': False})
                

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        contract_key= ContractHelper.makeRedisKeyEx(contract)
        self.update_portfolio_account(
                              {'tws_event': 'updatePortfolio',
                                'contract_key': contract_key, 
                                'position': position, 'market_price': marketPrice,
                                'market_value': marketValue, 'average_cost': averageCost, 
                                'unrealized_PNL': unrealizedPNL, 'realized_PNL': realizedPNL, 
                                'account': accountName}
                              )
        logging.info('TWS_event_handler:updatePortfolio. [%s]:position= %d' % (contract_key, position))
#         self.broadcast_event('updatePortfolio', {
#                                 'contract_key': contract_key, 
#                                 'position': position, 'market_price': marketPrice,
#                                 'market_value': marketValue, 'average_cost': averageCost, 
#                                 'unrealized_PNL': unrealizedPNL, 'realized_PNL': realizedPNL, 
#                                 'account': accountName,
#                                 'batch_end': False
#                                 })
                

    def updateAccountTime(self, timeStamp):
        self.update_portfolio_account({'tws_event':'updateAccountTime', 'timestamp': timeStamp})        
#        self.broadcast_event('updateAccountTime', {'timestamp': timeStamp, 'batch_end': False})
                

    def accountDownloadEnd(self, accountName):
        self.update_portfolio_account({'tws_event':'accountDownloadEnd', 'account':accountName})
#        self.broadcast_event('accountDownloadEnd', {'account':accountName})
        
        
    def nextValidId(self, orderId):
        self.broadcast_event('nextValidId', vars())

    def contractDetails(self, reqId, contractDetails):
        self.broadcast_event('contractDetails', vars())

    def contractDetailsEnd(self, reqId):
        self.broadcast_event('contractDetailsEnd', vars())

    def bondContractDetails(self, reqId, contractDetails):
        self.broadcast_event('bondContractDetails', vars())

    def execDetails(self, reqId, contract, execution):
        self.broadcast_event('execDetails', {'req_id': reqId, 'contract': contract, 'execution': execution, 'end_batch': False})

    def execDetailsEnd(self, reqId):
        self.broadcast_event('execDetails', {'req_id': reqId, 'contract': None, 'execution': None, 'end_batch': True})

    def connectionClosed(self):
        logging.warn('TWS_event_handler: connectionClosed ******')
        self.broadcast_event('connectionClosed', {})

    def error(self, id=None, errorCode=None, errorMsg=None):
        try:
            logging.error('TWS_event_handler:error. id:%s, errorCode:%s, errorMsg:%s' % (id, errorCode, errorMsg))
            self.broadcast_event('error', {'id': id, 
                                           'errorCode': errorCode, 'errorMsg': '%s(%s)' % (str(type(errorMsg)), str(errorMsg)) })

        except:
            pass

    def error_0(self, strvalue=None):
        logging.error(self.pre_process_message('error_0', vars()))
        self.broadcast_event('error_0', vars())

    def error_1(self, id=None, errorCode=None, errorMsg=None):
        logging.error(self.pre_process_message('error_1', vars()))        
        self.broadcast_event('error_1', vars())

    def updateMktDepth(self, tickerId, position, operation, side, price, size):
        self.broadcast_event('updateMktDepth', vars())

    def updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
        self.broadcast_event('updateMktDepthL2', vars())

    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        self.broadcast_event('updateNewsBulletin', vars())

    def managedAccounts(self, accountsList):
        logging.info(self.pre_process_message('managedAccounts', vars()))
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
        contract_key= ContractHelper.makeRedisKeyEx(contract)
        logging.info('TWS_event_handler:position. [%s]:position= %d' % (contract_key, pos))
        self.broadcast_event('position', {
                                'account': account,
                                'contract_key': contract_key, 
                                'position': pos, 'average_cost': avgCost,
                                'end_batch': False
                                
                                })        

    def positionEnd(self):
        '''
            positionEnd is sent to the client side as a 'position' event
            this is to mimick TWS behavior such that positionEnd is always the last message to send
            to the client as part of the reqPosition operation
            
            
            kafka does not guarantee the arrival sequence amongst different topics. Therefore
            if positionEnd is sent as a separate topic, it may arrive first before other
            position messages have been received by the client
            
            the 'end_batch' keyword is set to true 
        '''
        self.broadcast_event('position', {
                                'account': None,
                                'contract_key': None, 
                                'position': None, 'average_cost': None,
                                'end_batch': True
                                
                                })        


    def accountSummary(self, reqId, account, tag, value, currency):
        self.broadcast_event('accountSummary', vars())

    def accountSummaryEnd(self, reqId):
        self.broadcast_event('accountSummaryEnd', vars())
