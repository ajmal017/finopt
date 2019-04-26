#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import copy
from time import sleep, strftime
import time, datetime
import ConfigParser
from optparse import OptionParser
import logging
import thread
import threading
import traceback
import json
from threading import Lock
from ib.ext.Contract import Contract
from ib.ext.EWrapper import EWrapper
from ib.ext.EClientSocket import EClientSocket
from ib.ext.ExecutionFilter import ExecutionFilter
from ib.ext.Execution import Execution
from ib.ext.OrderState import OrderState
from ib.ext.Order import Order

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError


from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
from comms.ib_heartbeat import IbHeartBeat

from comms.test.base_messaging import Prosumer, BaseMessageListener
from misc2.observer import Subscriber, Publisher
from tws_protocol_helper import TWS_Protocol 

import redis
         

        
        
class TWS_event_handler(EWrapper):

    TICKER_GAP = 1000
    producer = None
    
    def __init__(self, producer):        
        self.producer = producer
 
 

 
    def broadcast_event(self, message, mapping):

        try:
            dict = self.tick_process_message(message, mapping)     
            if message == 'gw_subscriptions' or message == 'gw_subscription_changed':   
                logging.info('TWS_event_handler: broadcast event: %s [%s]' % (dict['typeName'], dict))
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
        
        self.broadcast_event('tickPrice', vars())

    def tickSize(self, tickerId, field, size):
        
        self.broadcast_event('tickSize', vars()) #vars())

    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        
        #self.broadcast_event('tickOptionComputation', self.tick_process_message(vars())) #vars())
        pass

    def tickGeneric(self, tickerId, tickType, value):
        self.broadcast_event('tickGeneric', vars()) 

    def tickString(self, tickerId, tickType, value):
        self.broadcast_event('tickString', vars()) 

    def tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        self.broadcast_event('tickEFP', vars())

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
            self.broadcast_event('error', vars())
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
        self.broadcast_event('position', vars())

    def positionEnd(self):
        self.broadcast_event('positionEnd', vars())

    def accountSummary(self, reqId, account, tag, value, currency):
        self.broadcast_event('accountSummary', vars())

    def accountSummaryEnd(self, reqId):
        self.broadcast_event('accountSummaryEnd', vars())



class TWS_gateway():

    
    # monitor IB connection / heart beat
#     ibh = None
#     tlock = None
#     ib_conn_status = None
    TWS_GW_DEFAULT_CONFIG = {
      'name': 'tws_gateway_server',
      'bootstrap_host': 'localhost',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'localhost',
      'tws_api_port': 8496,
      'group_id': 'TWS_GW',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'order_transmit': False
      }
               
    
    def __init__(self, kwargs):
        

             
        
        self.kwargs = copy.copy(TWS_gateway.TWS_GW_DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in kwargs:
                self.kwargs[key] = kwargs.pop(key)        
        self.kwargs.update(kwargs)        
        
        


        '''
            TWS_gateway start up sequence
            
            1. establish redis connection
            2. initialize prosumer instance - gateway message handler
            3. establish TWS gateway connectivity
            
            4. initialize listeners: ClientRequestHandler and SubscriptionManager
            5. start the prosumer 
        
        '''

        logging.info('starting up TWS_gateway...')
        self.ib_order_transmit = self.kwargs['order_transmit']
        logging.info('Order straight through (no-touch) flag = %s' % ('True' if self.ib_order_transmit == True else 'False'))
        
        
        logging.info('establishing redis connection...')
        self.initialize_redis()
        
        logging.info('starting up gateway message handler - kafka Prosumer...')        
        self.gw_message_handler = Prosumer(name='tws_gw_prosumer', kwargs=self.kwargs)
        
        logging.info('initializing TWS_event_handler...')        
        self.tws_event_handler = TWS_event_handler(self.gw_message_handler)
        
        logging.info('starting up IB EClientSocket...')
        self.tws_connection = EClientSocket(self.tws_event_handler)
        
        logging.info('establishing TWS gateway connectivity...')
        if not self.connect_tws():
            logging.error('TWS_gateway: unable to establish connection to IB %s:%d' % 
                          (self.kwargs['tws_host'], self.kwargs['tws_api_port']))
            self.disconnect_tws()
            sys.exit(-1)
        else:
            # start heart beat monitor
            pass
#             logging.info('starting up IB heart beat monitor...')
#             self.tlock = Lock()
#             self.ibh = IbHeartBeat(config)
#             self.ibh.register_listener([self.on_ib_conn_broken])
#             self.ibh.run()  

        logging.info('start TWS_event_handler. Entering processing loop...')
        self.gw_message_handler.start_prosumer()

        logging.info('instantiating listeners...cli_req_handler')        
        self.cli_req_handler = ClientRequestHandler('client_request_handler', self)
        logging.info('instantiating listeners subscription manager...')
        self.initialize_subscription_mgr()
        logging.info('registering messages to listen...')
        self.gw_message_handler.add_listeners([self.cli_req_handler])
        self.gw_message_handler.add_listener_topics(self.contract_subscription_mgr, ['reqMktData'])

        logging.info('**** Completed initialization sequence. ****')
        self.main_loop()
        

    def initialize_subscription_mgr(self):
        
        self.contract_subscription_mgr = SubscriptionManager(self, self)
        self.contract_subscription_mgr.register_persistence_callback(self.persist_subscriptions)
        
        
        key = self.kwargs["subscription_manager.subscriptions.redis_key"]
        if self.rs.get(key):
            #contracts = map(lambda x: ContractHelper.kvstring2contract(x), json.loads(self.rs.get(key)))
            
            def is_outstanding(c):
                
                today = time.strftime('%Y%m%d') 
                if c.m_expiry < today:
                    logging.info('initialize_subscription_mgr: ignoring expired contract %s%s%s' % (c.m_expiry, c.m_strike, c.m_right))
                    return False
                return True
            
            contracts = filter(lambda x: is_outstanding(x), 
                               map(lambda x: ContractHelper.kvstring2object(x, Contract), json.loads(self.rs.get(key))))
            
            
            
            
            self.contract_subscription_mgr.load_subscription(contracts)
        

    def persist_subscriptions(self, contracts):
         
        key = self.kwargs["subscription_manager.subscriptions.redis_key"]
        #cs = json.dumps(map(lambda x: ContractHelper.contract2kvstring(x) if x <> None else None, contracts))
        cs = json.dumps(map(lambda x: ContractHelper.object2kvstring(x) if x <> None else None, contracts))
        logging.debug('Tws_gateway: updating subscription table to redis store %s' % cs)
        self.rs.set(key, cs)


    def initialize_redis(self):

        self.rs = redis.Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        try:
            self.rs.client_list()
        except redis.ConnectionError:
            logging.error('TWS_gateway: unable to connect to redis server using these settings: %s port:%d db:%d' % 
                          (self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db']))
            logging.error('aborting...')
            sys.exit(-1)
            

    def connect_tws(self):
        logging.info('TWS_gateway - eConnect. Connecting to %s:%d App Id: %d...' % 
                     (self.kwargs['tws_host'], self.kwargs['tws_api_port'], self.kwargs['tws_app_id']))
        self.tws_connection.eConnect(self.kwargs['tws_host'], self.kwargs['tws_api_port'], self.kwargs['tws_app_id'])
        
        return self.tws_connection.isConnected()

    def disconnect_tws(self, value=None):
        sleep(2)
        self.tws_connection.eDisconnect()




    def on_ib_conn_broken(self, msg):
        logging.error('TWS_gateway: detected broken IB connection!')
        self.ib_conn_status = 'ERROR'
        self.tlock.acquire() # this function may get called multiple times
        try:                 # block until another party finishes executing
            if self.ib_conn_status == 'OK': # check status
                return                      # if already fixed up while waiting, return 
            
            self.eDisconnect()
            self.eConnect()
            while not self.tws_connection.isConnected():
                logging.error('TWS_gateway: attempt to reconnect...')
                self.eConnect()
                sleep(2)
            
            # we arrived here because the connection has been restored
            # resubscribe tickers again!
            logging.info('TWS_gateway: IB connection restored...resubscribe contracts')
            self.contract_subscription_mgr.force_resubscription()             
            
            
        finally:
            self.tlock.release()          
        

    def main_loop(self):
        try:
            logging.info('TWS_gateway:main_loop ***** accepting console input...')
            while True: 
                
                time.sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('TWS_gateway: caught user interrupt. Shutting down...')
                self.gw_message_handler.set_stop()
                self.gw_message_handler.join()
                logging.info('TWS_gateway: Service shut down complete...')
                sys.exit(0)        

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
            
            
    
        
class SubscriptionManager(BaseMessageListener):
    
    
    persist_f = None
    
    def __init__(self, name, tws_gateway):
        BaseMessageListener.__init__(self, name)
        self.tws_connect = tws_gateway.tws_connection
        self.handle = []    
        # contract key map to contract ID (index of the handle array)
        self.tickerId = {}
   
        
        
    def load_subscription(self, contracts):
        for c in contracts:
            self.reqMktData(c)
            
        self.dump()
    
    # returns -1 if not found, else the key id (which could be a zero value)
    def is_subscribed(self, contract):
        #print self.conId.keys()
        ckey = ContractHelper.makeRedisKeyEx(contract) 
        if ckey not in self.tickerId.keys():
            return -1
        else:
            # note that 0 can be a key 
            # be careful when checking the return values
            # check for true false instead of using implicit comparsion
            return self.tickerId[ckey]
    

#     def reqMktDataxx(self, contract):
#         print '---------------'
#         contractTuple = ('USO', 'STK', 'SMART', 'USD', '', 0.0, '')
#         stkContract = self.makeStkContract(contractTuple)     
#         stkContract.m_includeExpired = False       
#         self.parent.connection.reqMktData(1, stkContract, '', False)     
# 
#         contractTuple = ('IBM', 'STK', 'SMART', 'USD', '', 0.0, '')
#         stkContract = self.makeStkContract(contractTuple)
#         stkContract.m_includeExpired = False
#         print stkContract   
#         print stkContract.__dict__         
#         self.parent.connection.reqMktData(2, stkContract, '', False)     
#             

            
    def reqMktData(self, contract):
                  
        
        #logging.info('SubscriptionManager: reqMktData')
  
        def add_subscription(contract):
            self.handle.append(contract)
            newId = len(self.handle) - 1
            self.tickerId[ContractHelper.makeRedisKeyEx(contract)] = newId 
             
            return newId
  
        id = self.is_subscribed(contract)
        if id == -1: # not found
            id = add_subscription(contract)

            #
            # the conId must be set to zero when calling TWS reqMktData
            # otherwise TWS will fail to subscribe the contract
            
            self.tws_connect.reqMktData(id, contract, '', False) 
            
            
                   
            if self.persist_f:
                logging.debug('SubscriptionManager reqMktData: trigger callback')
                self.persist_f(self.handle)
                
            logging.info('SubscriptionManager: reqMktData. Requesting market data, id = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        else:    
            self.tws_connect.reqMktData(1000 + id, contract, '', True)
            logging.info('SubscriptionManager: reqMktData: contract already subscribed. Request snapshot = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        #self.dump()

        #
        # instruct gateway to broadcast new id has been assigned to a new contract
        #
        self.producer.send_message('gw_notify_subscription_changed', self.producer.message_dumps({id: ContractHelper.object2kvstring(contract)}))
        #>>>self.parent.gw_notify_subscription_changed({id: ContractHelper.object2kvstring(contract)})
        logging.info('SubscriptionManager reqMktData: gw_notify_subscription_changed: %d:%s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        
        
#     def makeStkContract(self, contractTuple):
#         newContract = Contract()
#         newContract.m_symbol = contractTuple[0]
#         newContract.m_secType = contractTuple[1]
#         newContract.m_exchange = contractTuple[2]
#         newContract.m_currency = contractTuple[3]
#         newContract.m_expiry = contractTuple[4]
#         newContract.m_strike = contractTuple[5]
#         newContract.m_right = contractTuple[6]
#         print 'Contract Values:%s,%s,%s,%s,%s,%s,%s:' % contractTuple
#         return newContract        
   
    # use only after a broken connection is restored
    # to re request market data 
    def force_resubscription(self):
        # starting from index 1 of the contract list, call  reqmktdata, and format the result into a list of tuples
        for i in range(1, len(self.handle)):
            self.tws_connect.reqMktData(i, self.handle[i], '', False)
            logging.info('force_resubscription: %s' % ContractHelper.printContract(self.handle[i]))
       
            
    def itemAt(self, id):
        if id > 0 and id < len(self.handle):
            return self.handle[id]
        return -1

    def dump(self):
        
        logging.info('subscription manager table:---------------------')
        logging.info(''.join('%d: {%s},\n' % (i,  ''.join('%s:%s, ' % (k, v) for k, v in self.handle[i].__dict__.iteritems() )\
                                     if self.handle[i] <> None else ''          ) for i in range(len(self.handle)))\
                     )
        
        #logging.info( ''.join('%s[%d],\n' % (k, v) for k, v in self.conId.iteritems()))
        logging.info( 'Number of instruments subscribed: %d' % len(self.handle))
        logging.info( '------------------------------------------------')
        
    def register_persistence_callback(self, func):
        logging.info('subscription manager: registering callback')
        self.persist_f = func
        

    
        



                
    
def test_subscription():        
    s = SubscriptionManager()
    contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20151029', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    print s.is_subscribed(c)
    print s.add_subscription(c)
    print s.is_subscribed(c)
    s.dump()
    
    fr = open('/home/larry-13.04/workspace/finopt/data/subscription-hsio.txt')
    for l in fr.readlines():
        if l[0] <> '#':
             
            s.add_subscription(ContractHelper.makeContract(tuple([t for t in l.strip('\n').split(',')])))    
    fr.close()
    s.dump()
    
    fr = open('/home/larry-13.04/workspace/finopt/data/subscription-hsio.txt')
    for l in fr.readlines():
        if l[0] <> '#':
             
            print s.add_subscription(ContractHelper.makeContract(tuple([t for t in l.strip('\n').split(',')])))    
    s.dump()
    contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20151127', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    print s.is_subscribed(c)
    print s.add_subscription(c)
    print s.is_subscribed(c), ContractHelper.printContract(s.itemAt(s.is_subscribed(c))) 
    
    print 'test itemAt:'
    contractTuple = ('HSI', 'OPT', 'HKFE', 'HKD', '20151127', 21400, 'C')
    c = ContractHelper.makeContract(contractTuple)
    print s.is_subscribed(c), ContractHelper.printContract(s.itemAt(s.is_subscribed(c)))
    
    

    
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
    
    
    app = TWS_gateway(kwargs)
    
     

#     


    


#    test_subscription()
    