#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import logging
import ConfigParser
from time import sleep
import time, datetime
import sleekxmpp
from threading import Lock
from kafka.client import KafkaClient
from kafka import KafkaConsumer
from kafka.producer import SimpleProducer
from kafka.common import LeaderNotAvailableError
import threading
from options_data import ContractHelper


        
class KConsumer(threading.Thread):
    
    producer = None
    consumer = None
    kwrapper = None

    def __init__(self, config):

        super(KConsumer, self).__init__()
        host = config.get("epc", "kafka.host").strip('"').strip("'")
        port = config.get("epc", "kafka.port")

        client = KafkaClient('%s:%s' % (host, port))
        self.producer = SimpleProducer(client, async=False)
        
        topicsEvents = ['accountDownloadEnd', 'execDetailsEnd', 'updateAccountTime', 'deltaNeutralValidation', 'orderStatus',\
                  'updateAccountValue', 'historicalData', 'openOrderEnd', 'updatePortfolio', 'managedAccounts', 'contractDetailsEnd',\
                  'positionEnd', 'bondContractDetails', 'accountSummary', 'updateNewsBulletin', 'scannerParameters', \
                  'tickString', 'accountSummaryEnd', 'scannerDataEnd', 'commissionReport', 'error', 'tickGeneric', 'tickPrice', \
                  'nextValidId', 'openOrder', 'realtimeBar', 'contractDetails', 'execDetails', 'tickOptionComputation', \
                  'updateMktDepth', 'scannerData', 'currentTime', 'error_0', 'error_1', 'tickSnapshotEnd', 'tickSize', \
                  'receiveFA', 'connectionClosed', 'position', 'updateMktDepthL2', 'fundamentalData', 'tickEFP']        
        
 
        self.consumer = KafkaConsumer( *[(v,0) for v in topicsEvents], \
                                       metadata_broker_list=['%s:%s' % (host, port)],\
                                       group_id = 'epc.group',\
                                       auto_commit_enable=True,\
                                       auto_commit_interval_ms=30 * 1000,\
                                       auto_offset_reset='largest') # discard old ones
        
        self.kwrapper= KWrapper(self.producer)

    def showmessage(self, msg_name, mapping):
        try:
            mapping = json.loads(mapping)
            del(mapping['self'])
        except (KeyError, ):
            pass
        items = list(mapping.items())
        items.sort()
        print(('### %s' % (msg_name, )))
        for k, v in items:
            print(('    %s:%s' % (k, v)))
        #print mapping.items()
        #print mapping
        m = Message(**mapping)
        print m
        

    def accountDownloadEnd(self, items):
        self.showmessage("accountDownloadEnd", items)

    def execDetailsEnd(self, items):
        self.showmessage("execDetailsEnd", items)

    def updateAccountTime(self, items):
        self.showmessage("updateAccountTime", items)

    def deltaNeutralValidation(self, items):
        self.showmessage("deltaNeutralValidation", items)

    def orderStatus(self, items):
        self.showmessage("orderStatus", items)

    def updateAccountValue(self, items):
        self.showmessage("updateAccountValue", items)

    def historicalData(self, items):
        self.showmessage("historicalData", items)

    def openOrderEnd(self, items):
        self.showmessage("openOrderEnd", items)

    def updatePortfolio(self, items):
        self.showmessage("updatePortfolio", items)

    def managedAccounts(self, items):
        self.showmessage("managedAccounts", items)

    def contractDetailsEnd(self, items):
        self.showmessage("contractDetailsEnd", items)

    def positionEnd(self, items):
        self.showmessage("positionEnd", items)

    def bondContractDetails(self, items):
        self.showmessage("bondContractDetails", items)

    def accountSummary(self, items):
        self.showmessage("accountSummary", items)

    def updateNewsBulletin(self, items):
        self.showmessage("updateNewsBulletin", items)

    def scannerParameters(self, items):
        self.showmessage("scannerParameters", items)

    def tickString(self, items):
        self.showmessage("tickString", items)

    def accountSummaryEnd(self, items):
        self.showmessage("accountSummaryEnd", items)

    def scannerDataEnd(self, items):
        self.showmessage("scannerDataEnd", items)

    def commissionReport(self, items):
        self.showmessage("commissionReport", items)

    def error(self, items):
        self.showmessage("error", items)

    def tickGeneric(self, items):
        self.showmessage("tickGeneric", items)

    def tickPrice(self, items):
        self.showmessage("tickPrice", items)

    def nextValidId(self, items):
        self.showmessage("nextValidId", items)

    def openOrder(self, items):
        self.showmessage("openOrder", items)

    def realtimeBar(self, items):
        self.showmessage("realtimeBar", items)

    def contractDetails(self, items):
        self.showmessage("contractDetails", items)

    def execDetails(self, items):
        self.showmessage("execDetails", items)

    def tickOptionComputation(self, items):
        self.showmessage("tickOptionComputation", items)

    def updateMktDepth(self, items):
        self.showmessage("updateMktDepth", items)

    def scannerData(self, items):
        self.showmessage("scannerData", items)

    def currentTime(self, items):
        self.showmessage("currentTime", items)

    def error_0(self, items):
        self.showmessage("error_0", items)

    def error_1(self, items):
        self.showmessage("error_1", items)

    def tickSnapshotEnd(self, items):
        self.showmessage("tickSnapshotEnd", items)

    def tickSize(self, items):
        self.showmessage("tickSize", items)

    def receiveFA(self, items):
        self.showmessage("receiveFA", items)

    def connectionClosed(self, items):
        self.showmessage("connectionClosed", items)

    def position(self, items):
        self.showmessage("position", items)

    def updateMktDepthL2(self, items):
        self.showmessage("updateMktDepthL2", items)

    def fundamentalData(self, items):
        self.showmessage("fundamentalData", items)

    def tickEFP(self, items):
        self.showmessage("tickEFP", items)





#     def on_tickPrice(self, tickerId, field, price, canAutoExecute):
#         self.showmessage('tickPrice', vars())
# 
#     def on_tickSize(self, tickerId, field, size):
#         self.showmessage('tickSize', vars())
# 
#     def on_tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
#         self.showmessage('tickOptionComputation', vars())
# 
#     def on_tickGeneric(self, tickerId, tickType, value):
#         self.showmessage('tickGeneric', vars())
# 
#     def on_tickString(self, tickerId, tickType, value):
#         self.showmessage('tickString', vars())
# 
#     def on_tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
#         self.showmessage('tickEFP', vars())
# 
#     def on_orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeId):
#         self.showmessage('orderStatus', vars())
# 
#     def on_openOrder(self, orderId, contract, order, state):
#         self.showmessage('openOrder', vars())
# 
#     def on_openOrderEnd(self):
#         self.showmessage('openOrderEnd', vars())
# 
#     def on_updateAccountValue(self, key, value, currency, accountName):
#         self.showmessage('updateAccountValue', vars())
# 
#     def on_updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
#         self.showmessage('updatePortfolio', vars())
# 
#     def on_updateAccountTime(self, timeStamp):
#         self.showmessage('updateAccountTime', vars())
# 
#     def on_accountDownloadEnd(self, accountName):
#         self.showmessage('accountDownloadEnd', vars())
# 
#     def on_nextValidId(self, orderId):
#         self.showmessage('nextValidId', vars())
# 
#     def on_contractDetails(self, reqId, contractDetails):
#         self.showmessage('contractDetails', vars())
# 
#     def on_contractDetailsEnd(self, reqId):
#         self.showmessage('contractDetailsEnd', vars())
# 
#     def on_bondContractDetails(self, reqId, contractDetails):
#         self.showmessage('bondContractDetails', vars())
# 
#     def on_execDetails(self, reqId, contract, execution):
#         self.showmessage('execDetails', vars())
# 
#     def on_execDetailsEnd(self, reqId):
#         self.showmessage('execDetailsEnd', vars())
# 
#     def on_connectionClosed(self):
#         self.showmessage('connectionClosed', {})
# 
#     def on_error(self, id=None, errorCode=None, errorMsg=None):
#         self.showmessage('error', vars())
# 
#     def on_error_0(self, strvalue=None):
#         self.showmessage('error_0', vars())
# 
#     def on_error_1(self, id=None, errorCode=None, errorMsg=None):
#         self.showmessage('error_1', vars())
# 
#     def on_updateMktDepth(self, tickerId, position, operation, side, price, size):
#         self.showmessage('updateMktDepth', vars())
# 
#     def on_updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
#         self.showmessage('updateMktDepthL2', vars())
# 
#     def on_updateNewsBulletin(self, msgId, msgType, message, origExchange):
#         self.showmessage('updateNewsBulletin', vars())
# 
#     def on_managedAccounts(self, accountsList):
#         self.showmessage('managedAccounts', vars())
# 
#     def on_receiveFA(self, faDataType, xml):
#         self.showmessage('receiveFA', vars())
# 
#     def on_historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
#         self.showmessage('historicalData', vars())
# 
#     def on_scannerParameters(self, xml):
#         self.showmessage('scannerParameters', vars())
# 
#     def on_scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
#         self.showmessage('scannerData', vars())
# 
# 
#     def on_commissionReport(self, commissionReport):
#         self.showmessage('commissionReport', vars())
# 
# 
#     def on_currentTime(self, time):
#         self.showmessage('currentTime', vars())
# 
#     def on_deltaNeutralValidation(self, reqId, underComp):
#         self.showmessage('deltaNeutralValidation', vars())
# 
# 
#     def on_fundamentalData(self, reqId, data):
#         self.showmessage('fundamentalData', vars())
# 
#     def on_marketDataType(self, reqId, marketDataType):
#         self.showmessage('marketDataType', vars())
# 
# 
#     def on_realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
#         self.showmessage('realtimeBar', vars())
# 
#     def on_scannerDataEnd(self, reqId):
#         self.showmessage('scannerDataEnd', vars())
# 
# 
# 
#     def on_tickSnapshotEnd(self, reqId):
#         self.showmessage('tickSnapshotEnd', vars())
# 
# 
#     def on_position(self, account, contract, pos, avgCost):
#         self.showmessage('position', vars())
# 
#     def on_positionEnd(self):
#         self.showmessage('positionEnd', vars())
# 
#     def on_accountSummary(self, reqId, account, tag, value, currency):
#         self.showmessage('accountSummary', vars())
# 
#     def on_accountSummaryEnd(self, reqId):
#         self.showmessage('accountSummaryEnd', vars())



                


                            
    def run(self):
        
        for message in self.consumer:
            
            logging.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))

            print ("received %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))
            
            #self.on_tickPrice(message.value)
            getattr(self, message.topic, None)(message.value)
            



class Message(object):
    """ Base class for Message types.

    """

    

    def __init__(self, **kwds):
        """ Constructor.

        @param **kwds keywords and values for instance
        """
        
        
        for name in kwds.keys():
            setattr(self, name, kwds[name])
        
        
        #assert not kwds

    def __len__(self):
        """ x.__len__() <==> len(x)

        """
        return len(self.keys())

    def __str__(self):
        """ x.__str__() <==> str(x)

        """
        name = self.typeName
        items = str.join(', ', ['%s=%s' % item for item in self.items()])
        return '<%s%s>' % (name, (' ' + items) if items else '')

    def items(self):
        """ List of message (slot, slot value) pairs, as 2-tuples.

        @return list of 2-tuples, each slot (name, value)
        """
        return zip(self.keys(), self.values())

    def values(self):
        """ List of instance slot values.

        @return list of each slot value
        """
        return [getattr(self, key, None) for key in self.keys()]

    def keys(self):
        """ List of instance slots.

        @return list of each slot.
        """
        return self.__dict__


class KWrapper():
    
    producer = None
    def __init__(self, producer):
        self.producer = producer
        
    def reqAccountUpdates(self):
        
        self.post_msg('reqAccountUpdates', '1')

    def reqExecutions(self):
        
        self.post_msg('reqExecutions', '')


    def reqMktData(self, contract):
        self.post_msg('reqMktData', ContractHelper.contract2mapstring(contract))
        
        
    def post_msg(self, topic, msg):
        logging.info('post_msg %s' % msg)
        self.producer.send_messages(topic, msg)


def dummy():
    topicsEvents = ['accountDownloadEnd', 'execDetailsEnd', 'updateAccountTime', 'deltaNeutralValidation', 'orderStatus',\
          'updateAccountValue', 'historicalData', 'openOrderEnd', 'updatePortfolio', 'managedAccounts', 'contractDetailsEnd',\
          'positionEnd', 'bondContractDetails', 'accountSummary', 'updateNewsBulletin', 'scannerParameters', \
          'tickString', 'accountSummaryEnd', 'scannerDataEnd', 'commissionReport', 'error', 'tickGeneric', 'tickPrice', \
          'nextValidId', 'openOrder', 'realtimeBar', 'contractDetails', 'execDetails', 'tickOptionComputation', \
          'updateMktDepth', 'scannerData', 'currentTime', 'error_0', 'error_1', 'tickSnapshotEnd', 'tickSize', \
          'receiveFA', 'connectionClosed', 'position', 'updateMktDepthL2', 'fundamentalData', 'tickEFP']

    s = ''.join('\tdef %s(self, items):\n\t\tself.showmessage("%s", items)\n\n' % (s,s) for s in topicsEvents)
    return s

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

    e = KConsumer(config)
    e.start()
    e.kwrapper.reqAccountUpdates()
    e.kwrapper.reqExecutions()
    #USD,CASH,IDEALPRO,JPY,,0,
    contractTuple = ('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    
    e.kwrapper.reqMktData(c)
    contractTuple = ('USO', 'STK', 'SMART', 'USD', '', 0, '')
    c = ContractHelper.makeContract(contractTuple)   
    e.kwrapper.reqMktData(c)
    
#    print dummy()
#     kwargs = {"arg3": 3, "arg2": "two","arg1":5}
#     m = Message(**kwargs)
#     print m.items()
#     print m.arg3
    
    

    