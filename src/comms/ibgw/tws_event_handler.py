#!/usr/bin/env python
# -*- coding: utf-8 -*-
from misc2.helpers import ContractHelper, ExecutionHelper, OrderHelper, OrderStateHelper
from misc2.observer import Publisher
import logging
import traceback
from ib.ext.EWrapper import EWrapper
import json
from copy import deepcopy

        
class TWS_event_handler(EWrapper, Publisher):

    TICKER_GAP = 1000
    producer = None
    
    # Events that will get forwarded to 
    # any classes that is interested in listening
    # WebConsole is one such subscriber
    # it is interested in 
    PUBLISH_TWS_EVENTS = ['error', 'openOrder', 'openOrderEnd', 'orderStatus', 'openBound', 'tickPrice', 'tickSize',
                          'tickOptionComputation', 'position', 'accountSummary'
                          ]
    
    def __init__(self, producer):
        self.producer = producer

        # create an internal publisher to forward tws messages 
        # to WebConsole class                
        Publisher.__init__(self, TWS_event_handler.PUBLISH_TWS_EVENTS)
        
    
    
    def set_order_id_manager(self, order_id_manager):
        self.order_id_manager = order_id_manager
 
    def set_subscription_manager(self, subscription_manager):
        self.subscription_manger = subscription_manager
 
    def broadcast_event(self, message, mapping):

        try:
            dict = self.pre_process_message(message, mapping)     
            logging.debug('broadcast_event %s:%s' % (message, dict))
            self.producer.send_message(message, self.producer.message_dumps(dict))   
            
            # forward message to subscribed consumers,
            # that is webconsole / RESTAPI interfaces
            if message in self.PUBLISH_TWS_EVENTS:
                self.dispatch(message, dict) 
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
        greeks = vars().copy()
        del greeks['tickerId']
        del greeks['self']
        self.broadcast_event('tickOptionComputation', {'contract_key': self.subscription_manger.get_contract_by_id(tickerId), 
                                                       'greeks': greeks}) 
        

    def tickGeneric(self, tickerId, tickType, value):
        #self.broadcast_event('tickGeneric', vars())
        pass 

    def tickString(self, tickerId, tickType, value):
        #self.broadcast_event('tickString', vars())
        pass 

    def tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        #self.broadcast_event('tickEFP', vars())
        pass

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        self.broadcast_event('orderStatus', {'orderId':orderId, 'status': status, 'filled':filled, 
                                             'remaining':remaining, 'avgFillPrice':avgFillPrice, 
                                             'permId':permId, 'parentId':parentId, 'lastFillPrice':lastFillPrice, 
                                             'clientId':clientId, 'whyHeld':whyHeld})

    def openOrder(self, orderId, contract, order, state):
        logging.info('TWS_event_handler: openOrder id:%d contract:%s' % (orderId, ContractHelper.makeRedisKey(contract)))
        logging.info('TWS_event_handler: openOrder order:%s' % (OrderHelper.object2kvstring(order)))
        logging.info('TWS_event_handler: openOrder state: %s' % (OrderStateHelper.object2kvstring(state)))
        self.broadcast_event('openOrder', {'orderId': orderId, 'contract':contract, 'order':order, 'state':state})
        

    def openOrderEnd(self):
        logging.info('TWS_event_handler: openOrderEnd')
        self.broadcast_event('openOrderEnd', {})

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
        #self.broadcast_event('nextValidId', orderId)
        self.order_id_manager.update_next_valid_id(orderId)
        logging.info('TWS_event_handler:nextValidId %d' % orderId)
        

    def contractDetails(self, reqId, contractDetails):
        self.broadcast_event('contractDetails', vars())

    def contractDetailsEnd(self, reqId):
        self.broadcast_event('contractDetailsEnd', vars())

    def bondContractDetails(self, reqId, contractDetails):
        self.broadcast_event('bondContractDetails', vars())

    def execDetails(self, reqId, contract, execution):
        contract_key= ContractHelper.makeRedisKeyEx(contract)
        self.broadcast_event('execDetails', {'req_id': reqId,
                                             'contract_key': contract_key, 
                                             'order_id': execution.m_orderId,
                                             'side': execution.m_side,
                                             'price': execution.m_price,
                                             'avg_price': execution.m_avgPrice,
                                             'cum_qty': execution.m_cumQty,
                                             'exec_id': execution.m_execId,
                                             'account': execution.m_acctNumber,
                                             'exchange': execution.m_exchange,
                                             'order_ref':execution.m_orderRef,
                                             'exec_time': execution.m_time,
                                             'end_batch': False})
        
        logging.info('TWS_event_handler:execDetails. [%s] execution id [%s]:= exec px %f' % (execution.m_acctNumber, execution.m_execId , execution.m_price))

                                     
    def execDetailsEnd(self, reqId):
        
        self.broadcast_event('execDetails', {'req_id': None, 
                                             'contract_key': None, 
                                             'order_id': None,
                                             'side': None,
                                             'price': None,
                                             'avg_price': None,
                                             'cum_qty': None,
                                             'exec_id': None,
                                             'account': None,
                                             'exchange': None,
                                             'order_ref':None,
                                             'exec_time': None, 
                                             'end_batch': True})
        
                  


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
        v = {'reqId': reqId, 'account': account, 'tag': tag, 'value': value, 'currency': currency, 'end_batch': False}
        self.broadcast_event('accountSummary', v)

    def accountSummaryEnd(self, reqId):
        v = {'reqId': reqId, 'end_batch': True}
        self.broadcast_event('accountSummary', v)
