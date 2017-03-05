#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import logging
import ConfigParser
from time import sleep
import time, datetime
from threading import Lock
#from kafka.client import KafkaClient


from kafka import KafkaProducer
from kafka import KafkaConsumer
#from kafka.producer import SimpleProducer
#from kafka.common import LeaderNotAvailableError, ConsumerTimeout
import threading

from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
import uuid


from tws_protocol_helper import TWS_Protocol, Message

        
class TWS_client_base_app(threading.Thread):
    
    producer = None
    consumer = None
    command_handler = None
    stop_consumer = False
    
    reg_all_callbacks = []
#    reg_event_func_map = {}


    def __init__(self, host, port, id=None):

        super(TWS_client_base_app, self).__init__()
        #client = KafkaClient('%s:%s' % (host, port))
        #self.producer = SimpleProducer(client, async=False)
        self.producer = KafkaProducer(bootstrap_servers='%s:%s' % (host, port))
        

 
        # consumer_timeout_ms must be set - this allows the consumer an interval to exit from its blocking loop
        self.consumer = KafkaConsumer( *[v for v in list(TWS_Protocol.topicEvents) + list(TWS_Protocol.gatewayEvents)] , \
                                       bootstrap_servers=['%s:%s' % (host, port)],\
                                       client_id = str(uuid.uuid1()) if id == None else id,\
                                       group_id = 'epc.group',\
                                       enable_auto_commit=True,\
                                       consumer_timeout_ms = 2000,\
                                       auto_commit_interval_ms=30 * 1000,\
                                       auto_offset_reset='latest') # discard old ones
        
        #self.reset_message_offset()
        
        #self.consumer.set_topic_partitions(('gw_subscriptions', 0, 114,),('tickPrice', 0, 27270,))
        self.command_handler= TWS_server_wrapper(self.producer)
        

    def reset_message_offset(self):
        # 90 is a magic number or don't care (max_num_offsets)
        topic_offsets =  map(lambda topic: (topic, self.consumer.get_partition_offsets(topic, 0, -1, 90)), TWS_Protocol.topicEvents + TWS_Protocol.gatewayEvents)
        topic_offsets = filter(lambda x: x <> None, map(lambda x: (x[0], x[1][1], max(x[1][0], 0)) if len(x[1]) > 1 else None, topic_offsets))
        logging.info("TWS_client_base_app: topic offset dump ------:")
        logging.info (topic_offsets)
        logging.info('TWS_client_base_app set topic offset to the latest point\n%s' % (''.join('%s,%s,%s\n' % (x[0], x[1], x[2]) for x in topic_offsets)))
        
        # the set_topic_partitions call clears out all previous settings when starts
        # therefore it's not possible to do something like this:
        # self.consumer.set_topic_partitions(('gw_subscriptions', 0, 114,)
        # self.consumer.set_topic_partitions(('tickPrice', 0, 27270,))
        # as the second call will wipe out whatever was done previously
        self.consumer.set_topic_partitions(*topic_offsets)        
        

    def get_producer(self):
        return self.producer
                            
    def run(self):

            
#             for message in self.consumer:
#                  
#                 logging.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                              message.offset, message.key,
#                                              message.value))
#      
#                 print ("received %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                              message.offset, message.key,
#                                              message.value))
#                  
#                 #self.on_tickPrice(message.value)
#                 getattr(self, message.topic, None)(message.value)
#                 # send message to the reg callback func pointers
#                 # the message is turned into IB compatible type before firing the callbacks
#                 [f(self.convertItemsToIBmessage(message.value)) for f in self.reg_all_callbacks]

            logging.info ('TWS_client_base_app: consumer_timeout_ms = %d' % self.consumer.config['consumer_timeout_ms']) 
 
            # keep running until someone tells us to stop
            while self.stop_consumer == False:
            #while True:
  
                    try:
                        # the next() function runs an infinite blocking loop
                        # it will raise a consumertimeout if no message is received after a pre-set interval   
                        message = self.consumer.next()
                        
                        if message.topic == 'gw_subscription_changed':
                            logging.info("TWS_client_base_app: %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
                        
                        kvmessage = self.convertItemsToIBmessage(message.value) 
                        
                        
                        getattr(self, message.topic, None)(kvmessage)
                        # send message to the reg callback func pointers
                        # the message is trasnformed into an IB compatible msg before firing the callbacks
                        [f(kvmessage) for f in self.reg_all_callbacks]                        
                        
                        #self.consumer.task_done(message)
                        
                    except: # ConsumerTimeout:
                        logging.info('TWS_client_base_app run: ConsumerTimeout. Check new message in the next round...')
                                        
                  
                

    def stop(self):
        logging.info('TWS_client_base_app: --------------- stopping consumer')
        self.stop_consumer = True
        

    def registerAll(self, funcs):
        [self.reg_all_callbacks.append(f) for f in funcs]
        
#     def register(self, event, func):
#         if event in TWS_Protocol.topicEvents:
#             self.reg_event_func_map[event] = [func] if self.reg_event_func_map[event] is None else self.reg_event_func_map[event].append(func)
#         else:
#             # raise exception
#             pass 

    def handlemessage(self, msg_name, mapping):

        items = list(mapping.items())
        items.sort()
#         print(('### %s' % (msg_name, )))
#         for k, v in items:
#             print(('    %s:%s' % (k, v)))
        print "TWS client base app: handlemessage<%s>: %s" % (msg_name, json.dumps(items))
    
    def ascii_encode_dict(self, data):
        ascii_encode = lambda x: x.encode('ascii') if isinstance(x, unicode) else x
        return dict(map(ascii_encode, pair) for pair in data.items())


    def convertItemsToIBmessage(self, items):
        # convert into our version of Message
        try:
            items = json.loads(items, object_hook=self.ascii_encode_dict)
            if 'contract' in items:
                items['contract'] = ContractHelper.kv2contract(items['contract'])
            del(items['self'])
        except (KeyError, ):
            #logging.error('convertItemsToIBmessage Exception: %s' % str(items))
            pass        
        return Message(**items)
    
    def get_command_handler(self):
        return self.command_handler

    def accountDownloadEnd(self, items):
        self.handlemessage("accountDownloadEnd", items)

    def execDetailsEnd(self, items):
        self.handlemessage("execDetailsEnd", items)

    def updateAccountTime(self, items):
        self.handlemessage("updateAccountTime", items)

    def deltaNeutralValidation(self, items):
        self.handlemessage("deltaNeutralValidation", items)

    def orderStatus(self, items):
        self.handlemessage("orderStatus", items)

    def updateAccountValue(self, items):
        self.handlemessage("updateAccountValue", items)

    def historicalData(self, items):
        self.handlemessage("historicalData", items)

    def openOrderEnd(self, items):
        self.handlemessage("openOrderEnd", items)

    def updatePortfolio(self, items):
        self.handlemessage("updatePortfolio", items)

    def managedAccounts(self, items):
        self.handlemessage("managedAccounts", items)

    def contractDetailsEnd(self, items):
        self.handlemessage("contractDetailsEnd", items)

    def positionEnd(self, items):
        self.handlemessage("positionEnd", items)

    def bondContractDetails(self, items):
        self.handlemessage("bondContractDetails", items)

    def accountSummary(self, items):
        self.handlemessage("accountSummary", items)

    def updateNewsBulletin(self, items):
        self.handlemessage("updateNewsBulletin", items)

    def scannerParameters(self, items):
        self.handlemessage("scannerParameters", items)

    def tickString(self, items):
        self.handlemessage("tickString", items)

    def accountSummaryEnd(self, items):
        self.handlemessage("accountSummaryEnd", items)

    def scannerDataEnd(self, items):
        self.handlemessage("scannerDataEnd", items)

    def commissionReport(self, items):
        self.handlemessage("commissionReport", items)

    def error(self, items):
        self.handlemessage("error", items)

    def tickGeneric(self, items):
        self.handlemessage("tickGeneric", items)

    def tickPrice(self, items):
        self.handlemessage("tickPrice", items)

    def nextValidId(self, items):
        self.handlemessage("nextValidId", items)

    def openOrder(self, items):
        self.handlemessage("openOrder", items)

    def realtimeBar(self, items):
        self.handlemessage("realtimeBar", items)

    def contractDetails(self, items):
        self.handlemessage("contractDetails", items)

    def execDetails(self, items):
        self.handlemessage("execDetails", items)

    def tickOptionComputation(self, items):
        self.handlemessage("tickOptionComputation", items)

    def updateMktDepth(self, items):
        self.handlemessage("updateMktDepth", items)

    def scannerData(self, items):
        self.handlemessage("scannerData", items)

    def currentTime(self, items):
        self.handlemessage("currentTime", items)

    def error_0(self, items):
        self.handlemessage("error_0", items)

    def error_1(self, items):
        self.handlemessage("error_1", items)

    def tickSnapshotEnd(self, items):
        self.handlemessage("tickSnapshotEnd", items)

    def tickSize(self, items):
        self.handlemessage("tickSize", items)

    def receiveFA(self, items):
        self.handlemessage("receiveFA", items)

    def connectionClosed(self, items):
        self.handlemessage("connectionClosed", items)

    def position(self, items):
        self.handlemessage("position", items)

    def updateMktDepthL2(self, items):
        self.handlemessage("updateMktDepthL2", items)

    def fundamentalData(self, items):
        self.handlemessage("fundamentalData", items)

    def tickEFP(self, items):
        self.handlemessage("tickEFP", items)


###########################################################################
#   Gateway respond events
#
#

    def gw_subscriptions(self, items):
        self.handlemessage("gw_subscriptions", items)


    def gw_subscription_changed(self, items):
        print '************************'
        self.handlemessage("gw_subscription_changed", items)

#     def on_tickPrice(self, tickerId, field, price, canAutoExecute):
#         self.handlemessage('tickPrice', vars())
# 
#     def on_tickSize(self, tickerId, field, size):
#         self.handlemessage('tickSize', vars())
# 
#     def on_tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
#         self.handlemessage('tickOptionComputation', vars())
# 
#     def on_tickGeneric(self, tickerId, tickType, value):
#         self.handlemessage('tickGeneric', vars())
# 
#     def on_tickString(self, tickerId, tickType, value):
#         self.handlemessage('tickString', vars())
# 
#     def on_tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
#         self.handlemessage('tickEFP', vars())
# 
#     def on_orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeId):
#         self.handlemessage('orderStatus', vars())
# 
#     def on_openOrder(self, orderId, contract, order, state):
#         self.handlemessage('openOrder', vars())
# 
#     def on_openOrderEnd(self):
#         self.handlemessage('openOrderEnd', vars())
# 
#     def on_updateAccountValue(self, key, value, currency, accountName):
#         self.handlemessage('updateAccountValue', vars())
# 
#     def on_updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
#         self.handlemessage('updatePortfolio', vars())
# 
#     def on_updateAccountTime(self, timeStamp):
#         self.handlemessage('updateAccountTime', vars())
# 
#     def on_accountDownloadEnd(self, accountName):
#         self.handlemessage('accountDownloadEnd', vars())
# 
#     def on_nextValidId(self, orderId):
#         self.handlemessage('nextValidId', vars())
# 
#     def on_contractDetails(self, reqId, contractDetails):
#         self.handlemessage('contractDetails', vars())
# 
#     def on_contractDetailsEnd(self, reqId):
#         self.handlemessage('contractDetailsEnd', vars())
# 
#     def on_bondContractDetails(self, reqId, contractDetails):
#         self.handlemessage('bondContractDetails', vars())
# 
#     def on_execDetails(self, reqId, contract, execution):
#         self.handlemessage('execDetails', vars())
# 
#     def on_execDetailsEnd(self, reqId):
#         self.handlemessage('execDetailsEnd', vars())
# 
#     def on_connectionClosed(self):
#         self.handlemessage('connectionClosed', {})
# 
#     def on_error(self, id=None, errorCode=None, errorMsg=None):
#         self.handlemessage('error', vars())
# 
#     def on_error_0(self, strvalue=None):
#         self.handlemessage('error_0', vars())
# 
#     def on_error_1(self, id=None, errorCode=None, errorMsg=None):
#         self.handlemessage('error_1', vars())
# 
#     def on_updateMktDepth(self, tickerId, position, operation, side, price, size):
#         self.handlemessage('updateMktDepth', vars())
# 
#     def on_updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
#         self.handlemessage('updateMktDepthL2', vars())
# 
#     def on_updateNewsBulletin(self, msgId, msgType, message, origExchange):
#         self.handlemessage('updateNewsBulletin', vars())
# 
#     def on_managedAccounts(self, accountsList):
#         self.handlemessage('managedAccounts', vars())
# 
#     def on_receiveFA(self, faDataType, xml):
#         self.handlemessage('receiveFA', vars())
# 
#     def on_historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
#         self.handlemessage('historicalData', vars())
# 
#     def on_scannerParameters(self, xml):
#         self.handlemessage('scannerParameters', vars())
# 
#     def on_scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
#         self.handlemessage('scannerData', vars())
# 
# 
#     def on_commissionReport(self, commissionReport):
#         self.handlemessage('commissionReport', vars())
# 
# 
#     def on_currentTime(self, time):
#         self.handlemessage('currentTime', vars())
# 
#     def on_deltaNeutralValidation(self, reqId, underComp):
#         self.handlemessage('deltaNeutralValidation', vars())
# 
# 
#     def on_fundamentalData(self, reqId, data):
#         self.handlemessage('fundamentalData', vars())
# 
#     def on_marketDataType(self, reqId, marketDataType):
#         self.handlemessage('marketDataType', vars())
# 
# 
#     def on_realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
#         self.handlemessage('realtimeBar', vars())
# 
#     def on_scannerDataEnd(self, reqId):
#         self.handlemessage('scannerDataEnd', vars())
# 
# 
# 
#     def on_tickSnapshotEnd(self, reqId):
#         self.handlemessage('tickSnapshotEnd', vars())
# 
# 
#     def on_position(self, account, contract, pos, avgCost):
#         self.handlemessage('position', vars())
# 
#     def on_positionEnd(self):
#         self.handlemessage('positionEnd', vars())
# 
#     def on_accountSummary(self, reqId, account, tag, value, currency):
#         self.handlemessage('accountSummary', vars())
# 
#     def on_accountSummaryEnd(self, reqId):
#         self.handlemessage('accountSummaryEnd', vars())








class TWS_server_wrapper():
    
    producer = None
    def __init__(self, producer):
        self.producer = producer


    
    def reqOpenOrders(self):
        self.post_msg('reqOpenOrders', '')
    
    
    def reqIds(self):
        self.post_msg('reqIds', '')
    
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
        self.post_msg('reqPositions', '')
        
    def reqHistoricalData(self):
        logging.error('reqHistoricalData: NOT IMPLEMENTED')
        

    def reqAccountUpdates(self):
        
        self.post_msg('reqAccountUpdates', '1')

    def reqExecutions(self, exec_filter=None):
        
        self.post_msg('reqExecutions', ExecutionFilterHelper.object2kvstring(exec_filter) if exec_filter <> None else '')


    def reqMktData(self, contract):
        #self.post_msg('reqMktData', ContractHelper.contract2kvstring(contract))
        self.post_msg('reqMktData', ContractHelper.object2kvstring(contract))
        
    def reqAccountSummary(self, reqId, group, tags):
        self.post_msg('reqAccountSummary', json.dumps([reqId, group, tags]))
    
    def placeOrder(self, id, contract, order):
        self.post_msg('placeOrder', json.dumps([id, ContractHelper.contract2kvstring(contract), OrderHelper.object2kvstring(order)]))
    
        
    def post_msg(self, topic, msg):
        logging.info('post_msg sending request to gateway: %s[%s]' % (topic,msg))
        self.producer.send(topic, msg)


#############################################################33
#  Gateway methods

    def gw_req_subscriptions(self):
        self.post_msg('gw_req_subscriptions', '')




class SimpleTWSClient(TWS_client_base_app):
    
    def __init__(self, host, port, id=None):
        super(SimpleTWSClient, self).__init__(host, port, id)
        logging.info('SimpleTWSClient client id=%s' % id)
    
    def connect(self):
        self.start()
    
    def disconnect(self):
        logging.info ('SimpleTWSClient: received disconnect. asking base class consumer to stop...')
        self.stop()
    




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0: 
        raise ValueError, "Failed to open config file" 
      
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

    host = config.get("epc", "kafka.host").strip('"').strip("'")
    port = config.get("epc", "kafka.port")


    e = SimpleTWSClient(host, port)
    #e.registerAll([e.on_ib_message])
    e.start()
    #e.command_handler.reqAccountUpdates()
    #e.command_handler.reqExecutions()
    #USD,CASH,IDEALPRO,JPY,,0,
    contractTuple = ('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    
    #e.command_handler.reqMktData(c)
    contractTuple = ('USO', 'STK', 'SMART', 'USD', '', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    #e.command_handler.reqMktData(c)
    e.get_command_handler().reqPositions()
#    print dummy()
#     kwargs = {"arg3": 3, "arg2": "two","arg1":5}
#     m = Message(**kwargs)
#     print m.items()
#     print m.arg3
    
    

    