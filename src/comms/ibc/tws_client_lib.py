#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import copy
from time import sleep, strftime
import ConfigParser
import logging
import json

from ib.ext.Contract import Contract

from misc2.helpers import ContractHelper, ExecutionFilterHelper, OrderHelper
from comms.ibgw.base_messaging import Prosumer, BaseMessageListener
from comms.tws_protocol_helper import TWS_Protocol
from misc2.observer import NotImplementedException
import redis
         
         
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
        self.producer.send_message('reqPositions', '')
        
    def reqHistoricalData(self):
        logging.error('reqHistoricalData: NOT IMPLEMENTED')
        
    def reqAccountUpdates(self):
        self.producer.send_message('reqAccountUpdates', '1')

    def reqExecutions(self, exec_filter=None):
        self.producer.send_message('reqExecutions', ExecutionFilterHelper.object2kvstring(exec_filter) if exec_filter <> None else '')

    def reqMktData(self, contract):
        self.producer.send_message('reqMktData', ContractHelper.object2kvstring(contract))
        
    def reqAccountSummary(self, reqId, group, tags):
        self.producer.send_message('reqAccountSummary', self.producer.message_dumps([reqId, group, tags]))
    
    def placeOrder(self, id, contract, order):
        self.producer.send_message('placeOrder', 
                                   self.producer.message_dumps([id, ContractHelper.contract2kvstring(contract), OrderHelper.object2kvstring(order)]))
    
        

    def gw_req_subscriptions(self, event, items):
        
        logging.info("[%s] received gw_req_subscriptions content:[%s]" % (self.name, items))
        vars= self.producer.message_loads(items['value'])
        self.producer.send_message('gw_req_subscriptions', self.producer.message_dumps(None))


         
class TWS_client_manager(GatewayCommandWrapper):

    
    TWS_CLI_DEFAULT_CONFIG = {
      'name': 'tws_gateway_client',
      'bootstrap_host': 'localhost',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'localhost',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'TWS_CLI',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      
      'topics': list(TWS_Protocol.topicEvents) + list(TWS_Protocol.gatewayEvents)
      }
      
               
    
    def __init__(self, kwargs):
        

        temp_kwargs = copy.copy(kwargs)
        self.kwargs = copy.copy(TWS_client_manager.TWS_CLI_DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in temp_kwargs:
                self.kwargs[key] = temp_kwargs.pop(key)        
        self.kwargs.update(temp_kwargs)        
        
        '''
            TWS_client_manager start up sequence
            
            1. establish redis connection
            2. initialize prosumer instance - gateway message handler
            
            4. initialize listeners: 
            5. start the prosumer 
        
        '''

        logging.info('starting up TWS_client_manager...')
        logging.info('establishing redis connection...')
        self.initialize_redis()
        
        logging.info('starting up gateway message handler - kafka Prosumer...')        
        self.gw_message_handler = Prosumer(name='tws_cli_prosumer', kwargs=self.kwargs)
        GatewayCommandWrapper.__init__(self, self.gw_message_handler)        



                
                
                    
        
        logging.info('**** Completed initialization sequence. ****')
        
        

    def start_manager(self):
        logging.info('start gw_message_handler. Entering processing loop...')
        self.gw_message_handler.start_prosumer()
    
    def add_listener_topics(self, listener, topics):
        self.gw_message_handler.add_listener_topics(listener, topics)

    def initialize_redis(self):

        self.rs = redis.Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        try:
            self.rs.client_list()
        except redis.ConnectionError:
            logging.error('TWS_client_manager: unable to connect to redis server using these settings: %s port:%d db:%d' % 
                          (self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db']))
            logging.error('aborting...')
            sys.exit(-1)
        

                                  
        
        

    
class AbstractGatewayListener(BaseMessageListener):
    
    def __init__(self, name):
        BaseMessageListener.__init__(self, name)
        
    
    def tickPrice(self, event, message_value):  # tickerId, field, price, canAutoExecute):
        """ generated source for method tickPrice """
        raise NotImplementedException
   
    def tickSize(self, event, message_value):  # tickerId, field, size):
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
   
    def updateAccountValue(self, event, message_value):  # key, value, currency, accountName):
        """ generated source for method updateAccountValue """
        raise NotImplementedException
   
    def updatePortfolio(self, event, message_value):  # contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        """ generated source for method updatePortfolio """
        raise NotImplementedException
   
    def updateAccountTime(self, event, message_value):  # timeStamp):
        """ generated source for method updateAccountTime """
        raise NotImplementedException
   
    def accountDownloadEnd(self, event, message_value):  # accountName):
        """ generated source for method accountDownloadEnd """
        raise NotImplementedException
   
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
   
    def execDetails(self, event, message_value):  # reqId, contract, execution):
        """ generated source for method execDetails """
        raise NotImplementedException
   
    def execDetailsEnd(self, event, message_value):  # reqId):
        """ generated source for method execDetailsEnd """
        raise NotImplementedException
   
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
   
    def position(self, event, message_value):  # account, contract, pos, avgCost):
        """ generated source for method position """
        raise NotImplementedException
   
    def positionEnd(self, event, message_value):
        """ generated source for method positionEnd """
        raise NotImplementedException
   
    def accountSummary(self, event, message_value):  # reqId, account, tag, value, currency):
        """ generated source for method accountSummary """
        raise NotImplementedException
   
    def accountSummaryEnd(self, event, message_value):  # reqId):
        """ generated source for method accountSummaryEnd """
        raise NotImplementedException

    def gw_subscription_changed(self, event, message_value):  # event, items):
        raise NotImplementedException        
#         logging.info("[%s] received gw_subscription_changed content: [%s]" % (self.name, message_value))
        
    def gw_subscriptions(self, event, message_value):
        raise NotImplementedException        
      
    def on_kb_reached_last_offset(self, event, message_value):  # event, items):
        logging.info("[%s] received on_kb_reached_last_offset content: [%s]" % (self.name, message_value))
        print "on_kb_reached_last_offset [%s] %s" % (self.name, message_value)
    

    
class ConfigMap():
    
    def kwargs_from_file(self, path):
        cfg = ConfigParser.ConfigParser()            
        if len(cfg.read(path)) == 0: 
            raise ValueError, "Failed to open config file [%s]" % path 

        kwargs = {}
        for section in cfg.sections():
            optval_list = map(lambda o: (o, cfg.get(section, o)), cfg.options(section)) 
            for ov in optval_list:
                try:
                    
                    kwargs[ov[0]] = eval(ov[1])
                except:
                    continue
                
        #logging.debug('ConfigMap: %s' % kwargs)
        return kwargs
       
class GatewayMessageListener(AbstractGatewayListener):   
    def __init__(self, name):
        AbstractGatewayListener.__init__(self, name)
             
    def tickPrice(self, event, message_value):  # tickerId, field, price, canAutoExecute):
        logging.info('GatewayMessageListener:tickPrice. val->[%s]' % message_value)

def test_client(kwargs):
    contractTuple = ('USO', 'STK', 'SMART', 'USD', '', 0.0, '')
    contract = ContractHelper.makeContract(contractTuple)    
    print kwargs 
    cm = TWS_client_manager(kwargs)
    cl = AbstractGatewayListener('gw_client_message_listener')
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
    cm.reqMktData(contract)
    try:
        logging.info('TWS_gateway:main_loop ***** accepting console input...')
        while True: 
        
            sleep(.45)
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('TWS_client_manager: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop()
        cm.join()
        logging.info('TWS_client_manager: Service shut down complete...')
           
        
if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    



    cfg_path= sys.argv[1:]
    kwargs = ConfigMap().kwargs_from_file(cfg_path)
   
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    print ContractHelper.kvstring2object('{"m_conId": 0, "m_symbol": "USO", "m_secType": "STK", "m_includeExpired": false, "m_right": "", "m_expiry": "", "m_currency": "USD", "m_exchange": "SMART", "m_strike": 0.0}', Contract)
    #test_client(kwargs)
    
     
