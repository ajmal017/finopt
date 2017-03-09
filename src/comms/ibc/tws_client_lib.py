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
import redis
         
class TWS_user():

    
    # monitor IB connection / heart beat
#     ibh = None
#     tlock = None
#     ib_conn_status = None
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
        

             
        
        self.kwargs = copy.copy(TWS_user.TWS_CLI_DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in kwargs:
                self.kwargs[key] = kwargs.pop(key)        
        self.kwargs.update(kwargs)        
        
        


        '''
            TWS_user start up sequence
            
            1. establish redis connection
            2. initialize prosumer instance - gateway message handler
            
            4. initialize listeners: 
            5. start the prosumer 
        
        '''

        logging.info('starting up TWS_user...')
        
        
        logging.info('establishing redis connection...')
        self.initialize_redis()
        
        logging.info('starting up gateway message handler - kafka Prosumer...')        
        self.gw_message_handler = Prosumer(name='tws_cli_prosumer', kwargs=self.kwargs)
        
        logging.info('start gw_message_handler. Entering processing loop...')
        self.gw_message_handler.start_prosumer()

        logging.info('instantiating gw_command_proxy')        
        self.gw_send_command = GatewayCommandProxy('gw_send_command', self.gw_message_handler)
        logging.info('instantiating listeners subscription manager...')

                

        logging.info('**** Completed initialization sequence. ****')
        self.main_loop()
        


    def initialize_redis(self):

        self.rs = redis.Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        try:
            self.rs.client_list()
        except redis.ConnectionError:
            logging.error('TWS_user: unable to connect to redis server using these settings: %s port:%d db:%d' % 
                          (self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db']))
            logging.error('aborting...')
            sys.exit(-1)
        

    def main_loop(self):
        try:
            logging.info('TWS_user:main_loop ***** accepting console input...')
            while True: 
                
                sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('TWS_user: caught user interrupt. Shutting down...')
                self.gw_message_handler.set_stop()
                self.gw_message_handler.join()
                logging.info('TWS_user: Service shut down complete...')
                sys.exit(0)        



class GatewayCommandProxy():

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


                               
        
        

    
class GatewayMessageListener(BaseMessageListener):
    
    def __init__(self, name, producer):
        BaseMessageListener.__init__(self, name)
        self.producer = producer
  
    
    
    def tickPrice(self, event, message_value):  # tickerId, field, price, canAutoExecute):
        """ generated source for method tickPrice """
       
   
    def tickSize(self, event, message_value):  # tickerId, field, size):
        """ generated source for method tickSize """

   
    def tickOptionComputation(self, event, message_value):  # tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        """ generated source for method tickOptionComputation """

   
    def tickGeneric(self, event, message_value):  # tickerId, tickType, value):
        """ generated source for method tickGeneric """

   
    def tickString(self, event, message_value):  # tickerId, tickType, value):
        """ generated source for method tickString """

   
    def tickEFP(self, event, message_value):  # tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        """ generated source for method tickEFP """

   
    def orderStatus(self, event, message_value):  # orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        """ generated source for method orderStatus """

   
    def openOrder(self, event, message_value):  # orderId, contract, order, orderState):
        """ generated source for method openOrder """

   
    def openOrderEnd(self, event, message_value):
        """ generated source for method openOrderEnd """

   
    def updateAccountValue(self, event, message_value):  # key, value, currency, accountName):
        """ generated source for method updateAccountValue """

   
    def updatePortfolio(self, event, message_value):  # contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        """ generated source for method updatePortfolio """

   
    def updateAccountTime(self, event, message_value):  # timeStamp):
        """ generated source for method updateAccountTime """

   
    def accountDownloadEnd(self, event, message_value):  # accountName):
        """ generated source for method accountDownloadEnd """

   
    def nextValidId(self, event, message_value):  # orderId):
        """ generated source for method nextValidId """

   
    def contractDetails(self, event, message_value):  # reqId, contractDetails):
        """ generated source for method contractDetails """

   
    def bondContractDetails(self, event, message_value):  # reqId, contractDetails):
        """ generated source for method bondContractDetails """

   
    def contractDetailsEnd(self, event, message_value):  # reqId):
        """ generated source for method contractDetailsEnd """

   
    def execDetails(self, event, message_value):  # reqId, contract, execution):
        """ generated source for method execDetails """

   
    def execDetailsEnd(self, event, message_value):  # reqId):
        """ generated source for method execDetailsEnd """

   
    def updateMktDepth(self, event, message_value):  # tickerId, position, operation, side, price, size):
        """ generated source for method updateMktDepth """

   
    def updateMktDepthL2(self, event, message_value):  # tickerId, position, marketMaker, operation, side, price, size):
        """ generated source for method updateMktDepthL2 """

   
    def updateNewsBulletin(self, event, message_value):  # msgId, msgType, message, origExchange):
        """ generated source for method updateNewsBulletin """

   
    def managedAccounts(self, event, message_value):  # accountsList):
        """ generated source for method managedAccounts """

   
    def receiveFA(self, event, message_value):  # faDataType, xml):
        """ generated source for method receiveFA """

   
    def historicalData(self, event, message_value):  # reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        """ generated source for method historicalData """

   
    def scannerParameters(self, event, message_value):  # xml):
        """ generated source for method scannerParameters """

   
    def scannerData(self, event, message_value):  # reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        """ generated source for method scannerData """

   
    def scannerDataEnd(self, event, message_value):  # reqId):
        """ generated source for method scannerDataEnd """

   
    def realtimeBar(self, event, message_value):  # reqId, time, open, high, low, close, volume, wap, count):
        """ generated source for method realtimeBar """

   
    def currentTime(self, event, message_value):  # time):
        """ generated source for method currentTime """

   
    def fundamentalData(self, event, message_value):  # reqId, data):
        """ generated source for method fundamentalData """

   
    def deltaNeutralValidation(self, event, message_value):  # reqId, underComp):
        """ generated source for method deltaNeutralValidation """

   
    def tickSnapshotEnd(self, event, message_value):  # reqId):
        """ generated source for method tickSnapshotEnd """

   
    def marketDataType(self, event, message_value):  # reqId, marketDataType):
        """ generated source for method marketDataType """

   
    def commissionReport(self, event, message_value):  # commissionReport):
        """ generated source for method commissionReport """

   
    def position(self, event, message_value):  # account, contract, pos, avgCost):
        """ generated source for method position """

   
    def positionEnd(self, event, message_value):
        """ generated source for method positionEnd """

   
    def accountSummary(self, event, message_value):  # reqId, account, tag, value, currency):
        """ generated source for method accountSummary """

   
    def accountSummaryEnd(self, event, message_value):  # reqId):
        """ generated source for method accountSummaryEnd """


    def gw_subscription_changed(self, event, message_value):  # event, items):
        logging.info("[%s] received gw_subscription_changed content: [%s]" % (self.name, message_value))
        #print 'SubscriptionListener:gw_subscription_changed %s' % items
        
        
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
        
    
if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    



    cfg_path= sys.argv[1:]
    kwargs = ConfigMap().kwargs_from_file(cfg_path)
   
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    app = TWS_user(kwargs)
    
     
