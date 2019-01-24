# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread, threading
from threading import Lock
import ConfigParser


from time import sleep
import time, datetime
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.ExecutionFilter import ExecutionFilter
from random import randint

from finopt.options_analytics import AnalyticsListener


from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
from finopt.options_chain import OptionsChain
from comms.tws_client import SimpleTWSClient


from kafka import KafkaConsumer
from comms.tws_protocol_helper import TWS_Protocol



class SampleClient(SimpleTWSClient):

    tickerMap = {}
    

    def dump(self, msg_name, mapping):
        # the mapping is a comms.tws_protocol_helper.Message object
        # which can be accessed directly using the __dict__.['xxx'] method 
        items = list(mapping.items())
        items.sort()
        print ('>>> %s <<< %s' % (msg_name, ''.join('%s=%s, '% (k, v if k <> 'ts' else datetime.datetime.fromtimestamp(v).strftime('%Y-%m-%d %H:%M:%S.%f')) for k, v in items))) 
    
    def accountSummaryEnd(self, items):
        self.dump('accountSummaryEnd', items)
        
    def accountSummary(self, items):
        self.dump('accountSummary', items)
    # override the tickSize message
    def tickSize(self, items):
        try:
            contract = self.tickerMap[items.__dict__['tickerId']]
            field = items.__dict__['field']
            ct =   ContractHelper.kv2object(contract, Contract)
            print 'tickSize>> %s' % ('[%d:%s] %s=%0.4f [%s]' % \
                                        (items.__dict__['tickerId'], ContractHelper.makeRedisKeyEx(ct),\
                                        'bid' if field == 0 else ('ask' if field == 3 else ('last' if field == 5 else field)), \
                                        items.__dict__['size'], datetime.datetime.fromtimestamp(items.__dict__['ts']).strftime('%Y-%m-%d %H:%M:%S.%f')))
            
        except KeyError:
            print 'tickSize: keyerror: (this could happen on the 1st run as the subscription manager sub list is still empty.'
            print items


    def tickPrice(self, items):
        try:
            contract = self.tickerMap[items.__dict__['tickerId']]
            field = items.__dict__['field']
            ct =   ContractHelper.kv2object(contract, Contract)
            print 'tickPrice>> %s' % ('[%d:%s] %s=%0.4f [%s]' % \
                                        (items.__dict__['tickerId'], ContractHelper.makeRedisKeyEx(ct),\
                                        'bid_q' if field == 1 else ('ask_q' if field == 2 else ('last_q' if field == 4 else field)), \
                                        items.__dict__['price'], datetime.datetime.fromtimestamp(items.__dict__['ts']).strftime('%Y-%m-%d %H:%M:%S.%f')))
        except KeyError:
            print 'tickPrice: keyerror:'
            print items
            
            
    def tickString(self, items):
        pass

    def tickGeneric(self, items):
        pass

    
    def positionEnd(self, items):
        self.dump('positionEnd', items)
       

    def position(self, items):
        self.dump('position', items)
        #pass

    def error(self, items):
        self.dump('error', items)

    def error_0(self, items):
        self.dump('error', items)
 
    def error_1(self, items):
        self.dump('error', items)
        
    def gw_subscriptions(self, items):
        # <class 'comms.tws_protocol_helper.Message'>
        # sample
        #[[0, u'{"m_conId": 0, "m_right": "", "m_symbol": "HSI", "m_secType": "FUT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 0}'], [1, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 22200}'], [2, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 22400}'], [3, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 22600}'], [4, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 22800}'], [5, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 23000}'], [6, u'{"m_conId": 0, "m_right": "P", "m_symbol": "HSI", "m_secType": "OPT", "m_includeExpired": false, "m_expiry": "20151127", "m_currency": "HKD", "m_exchange": "HKFE", "m_strike": 23200}']]
        #print items.__dict__['subscriptions']
        
        #l = map(lambda x: {x[0]: x[1]}, map(lambda x: (x[0], ContractHelper.kvstring2object(x[1], Contract)), items.__dict__['subscriptions']))
        l = map(lambda x: {x[0]: x[1]}, map(lambda x: (x[0], json.loads(x[1])), items.__dict__['subscriptions']))
        for i in l:
            self.tickerMap.update(i)   
        print 'gw_subscriptions -> dump tickerMap '
        print self.tickerMap 
        
    def gw_subscription_changed(self, items):
        print '[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[['
        print items        
    
# override this function to perform your own processing
#    def accountDownloadEnd(self, items):
#        pass

# override this function to perform your own processing
#    def execDetailsEnd(self, items):
#        pass

# override this function to perform your own processing
#    def updateAccountTime(self, items):
#        pass

# override this function to perform your own processing
#    def deltaNeutralValidation(self, items):
#        pass

# override this function to perform your own processing
#    def orderStatus(self, items):
#        pass

# override this function to perform your own processing
#    def updateAccountValue(self, items):
#        pass

# override this function to perform your own processing
#    def historicalData(self, items):
#        pass

# override this function to perform your own processing
#    def openOrderEnd(self, items):
#        pass

# override this function to perform your own processing
#    def updatePortfolio(self, items):
#        pass

# override this function to perform your own processing
#    def managedAccounts(self, items):
#        pass

# override this function to perform your own processing
#    def contractDetailsEnd(self, items):
#        pass

# override this function to perform your own processing
#    def positionEnd(self, items):
#        pass

# override this function to perform your own processing
#    def bondContractDetails(self, items):
#        pass

# override this function to perform your own processing
#    def accountSummary(self, items):
#        pass

# override this function to perform your own processing
#    def updateNewsBulletin(self, items):
#        pass

# override this function to perform your own processing
#    def scannerParameters(self, items):
#        pass

# override this function to perform your own processing
#    def tickString(self, items):
#        pass

# override this function to perform your own processing
#    def accountSummaryEnd(self, items):
#        pass

# override this function to perform your own processing
#    def scannerDataEnd(self, items):
#        pass

# override this function to perform your own processing
#    def commissionReport(self, items):
#        pass

# override this function to perform your own processing
#    def error(self, items):
#        pass

# override this function to perform your own processing
#    def tickGeneric(self, items):
#        pass

# override this function to perform your own processing
#    def tickPrice(self, items):
#        pass

# override this function to perform your own processing
#    def nextValidId(self, items):
#        pass

# override this function to perform your own processing
#    def openOrder(self, items):
#        pass

# override this function to perform your own processing
#    def realtimeBar(self, items):
#        pass

# override this function to perform your own processing
#    def contractDetails(self, items):
#        pass

# override this function to perform your own processing
#    def execDetails(self, items):
#        pass

# override this function to perform your own processing
#    def tickOptionComputation(self, items):
#        pass

# override this function to perform your own processing
#    def updateMktDepth(self, items):
#        pass

# override this function to perform your own processing
#    def scannerData(self, items):
#        pass

# override this function to perform your own processing
#    def currentTime(self, items):
#        pass

# override this function to perform your own processing
#    def error_0(self, items):
#        pass

# override this function to perform your own processing
#    def error_1(self, items):
#        pass

# override this function to perform your own processing
#    def tickSnapshotEnd(self, items):
#        pass

# override this function to perform your own processing
#    def tickSize(self, items):
#        pass

# override this function to perform your own processing
#    def receiveFA(self, items):
#        pass

# override this function to perform your own processing
#    def connectionClosed(self, items):
#        pass

# override this function to perform your own processing
#    def position(self, items):
#        pass

# override this function to perform your own processing
#    def updateMktDepthL2(self, items):
#        pass

# override this function to perform your own processing
#    def fundamentalData(self, items):
#        pass

# override this function to perform your own processing
#    def tickEFP(self, items):
#        pass





def on_ib_message(msg):
    print msg






def makeOrder(action, orderID, tif, orderType, price, qty):
    newOptOrder = Order()
    newOptOrder.m_orderId = orderID
    newOptOrder.m_clientId = 0
    newOptOrder.m_permid = 0
    newOptOrder.m_action = action
    newOptOrder.m_lmtPrice = price
    newOptOrder.m_auxPrice = 0
    newOptOrder.m_tif = tif
    newOptOrder.m_transmit = True
    newOptOrder.m_orderType = orderType
    newOptOrder.m_totalQuantity = qty
    return newOptOrder







def test1():
    twsc = SimpleTWSClient(host, port, '888')
    twsc.registerAll([on_ib_message])
    #twsc.get_command_handler().reqAccountSummary(100, 'All', "AccountType,NetLiquidation,TotalCashValue,BuyingPower,EquityWithLoanValue")
     
     
    contract = Contract() #
    contract.m_symbol = 'WMT'
    contract.m_currency = 'USD'
    contract.m_secType = 'STK'
    contract.m_exchange = 'SMART'
    twsc.get_command_handler().reqMktData(contract)
      
    twsc.connect()
    sleep(4)
    twsc.disconnect()
    print 'completed...'


def test15():
    contract = Contract() #
    contract.m_symbol = 'WMT'
    contract.m_currency = 'USD'
    contract.m_secType = 'STK'
    contract.m_exchange = 'SMART'
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()

    c.get_command_handler().reqMktData(contract)
    sleep(4)
    c.disconnect()
    print 'completed...'
    
def test2():
    
    contract = Contract() #
    contract.m_symbol = 'EUR'
    contract.m_currency = 'USD'
    contract.m_secType = 'CASH'
    contract.m_exchange = 'IDEALPRO'
    
   
       
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    c.get_command_handler().gw_req_subscriptions()
    #c.get_command_handler().reqIds()
    c.get_command_handler().reqMktData(contract)
    
    for i in range(567,568):
        orderID = i
 
        order = makeOrder( 'SELL', i, 'DAY', 'LMT', 1.0, randint(10,20) * 1000)
        c.get_command_handler().placeOrder(orderID, contract, order)    
    
    
    sleep(3)
    c.get_command_handler().reqOpenOrders()
    c.get_command_handler().reqExecutions()
    c.get_command_handler().reqPositions()
    
    sleep(8)
    c.disconnect()
    print 'completed...'


def test3():


    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    
    
    
#     m_clientId = 0  # zero means no filtering on this field
#     m_acctCode = ""
#     m_time = ""
#     m_symbol = ""
#     m_secType = ""
#     m_exchange = ""
#     m_side = ""    
    filter = ExecutionFilterHelper.kv2object({'m_time': '20190122  09:35:00'}, ExecutionFilter) 
    c.get_command_handler().reqExecutions(filter)
    sleep(7)    
    
#"yyyymmdd-hh:mm:ss"    
    c.disconnect()



def test4():
    #global host, port
    
    f = open('/home/larry/l1304/workspace/finopt/data/subscription-nov-HSI.txt')
    lns = f.readlines()
    cs = map (lambda l: ContractHelper.makeContract(tuple(l.strip('\n').split(','))), lns)
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    #for contract in cs:
    #c.get_command_handler().reqMktData(cs[0])    
#         
#     contract = Contract() #
#     contract.m_symbol = 'EUR'
#     contract.m_currency = 'USD'
#     contract.m_secType = 'CASH'
#     contract.m_exchange = 'IDEALPRO'
#     c.get_command_handler().reqMktData(contract)
#     
#     contract.m_symbol = 'HSI'
#     contract.m_currency = 'HKD'
#     contract.m_secType = 'OPT'
#     contract.m_exchange = 'HKFE'
#     contract.m_strike = 21000
#     contract.m_multiplier = 50.0
#     contract.m_includeExpired = False
#     
#     contract.m_right = 'P'
#     contract.m_expiry = '20151127'
    contract = Contract()
    contract.m_symbol = 'GOOG'
    contract.m_currency = 'USD'
    contract.m_secType = 'STK'
    contract.m_exchange = 'SMART'
    #contract.m_strike = 58.5
    #contract.m_multiplier = 100
    #contract.m_includeExpired = False
    
    #contract.m_right = 'P'
    #contract.m_expiry = '20151120'    
    c.get_command_handler().reqMktData(contract)
    contract = Contract()
    contract.m_symbol = 'EWT'
    contract.m_currency = 'USD'
    contract.m_secType = 'STK'
    contract.m_exchange = 'SMART'    
    c.get_command_handler().reqMktData(contract)
    sleep(1)
    c.get_command_handler().gw_req_subscriptions()
    
    sleep(10)
    c.disconnect()


def test5():
    print '******************************** TEST 5'
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    #c.get_command_handler().reqIds()
    c.get_command_handler().gw_req_subscriptions()
    c.get_command_handler().reqExecutions()
    sleep(8)
    c.disconnect()
    
    
def test6():
    
    contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20151127', 0, '')
    #contractTuple = ('VMW', 'STK', 'SMART', 'USD', '', 0, '')
    contract = ContractHelper.makeContract(contractTuple)  
    oc = OptionsChain('test6')
    
    oc.set_underlying(contract)
    oc.set_option_structure(contract, 200, 50, 0.005, 0.003, '20151127')
    oc.build_chain(22300, 0.1)    
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    #c.get_command_handler().reqIds()
    c.get_command_handler().gw_req_subscriptions()
    

    c.get_command_handler().reqMktData(contract)
    for ct in oc.get_option_chain():    
        c.get_command_handler().reqMktData(ct.get_contract())
        print ContractHelper.object2kvstring(ct.get_contract())
    sleep(8)
    c.disconnect()
        
        
def test7():
    contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
    contract = ContractHelper.makeContract(contractTuple)  
    oc = OptionsChain('t7')
    
    
    oc.set_underlying(contract)
    
    # underlying, spd_size, multiplier, rate, div, expiry):   
    oc.set_option_structure(contract, 0.5, 100, 0.0012, 0.0328, '20170217')
    oc.build_chain(125, 0.04, 0.22)            
    
    c = SampleClient(host, port, 'SampleClient-777')
    c.connect()
    #c.get_command_handler().reqIds()
    c.get_command_handler().gw_req_subscriptions()
    

    c.get_command_handler().reqMktData(contract)
    for ct in oc.get_option_chain():    
        c.get_command_handler().reqMktData(ct.get_contract())
        print ContractHelper.object2kvstring(ct.get_contract())
    sleep(1)
    c.disconnect()
    
def test8():    
#     c = SampleClient(host, port, 'SampleClient-777')
#     c.connect()
    
    consumer = KafkaConsumer( *[(v,0) for v in list(TWS_Protocol.oceEvents)] , \
                                   metadata_broker_list=['%s:%s' % (host, port)],\
                                   client_id = 'test8',\
                                   group_id = 'epc.group',\
                                   auto_commit_enable=True,\
                                   consumer_timeout_ms = 2000,\
                                   auto_commit_interval_ms=30 * 1000,\
                                   auto_offset_reset='smallest')    
    
#     c.get_command_handler().gw_req_subscriptions()
    for message in consumer:
          

    
        print ("received %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                     message.offset, message.key,
                                     message.value))    
    
    print 'here'
#     c.disconnect()


def on_analytics(msg):
    print msg
    kv = json.loads(msg)
    print 'x=%s imvol=%0.2f theta=%0.2f' % (kv['contract']['m_strike'], kv['analytics']['imvol'],kv['analytics']['theta'])

def test9():
    al = AnalyticsListener(host, port, 'al')
    al.registerAll([on_analytics])
    al.start()
            
if __name__ == '__main__':
    """ 
        this sample demonstrates how to use SimpleTWSClient
        to connect to IB TWS gateway 
        
        
        
        usage scenarios:
        
        case 1
        re-use SimpleTWSClient object
        register to listen for all messages
        perform processing within the callback function
        
        case 2
        inherit SimpleTWSClient class
        override event callback functions to use
        each function associates with a specific message type
        
    """
    if len(sys.argv) != 2:
        print("Usage: %s <test case #>" % sys.argv[0])
        exit(-1)    

    choice= sys.argv[1]
           
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s',
                        filename= '/tmp/unitest.log')
    
    
    # bootstrap server settings
    host = 'vorsprung'
    port = 9092

    print 'choice: %s' % choice
    
    #test8()
    if choice == '2': 
         
        test2()
    else:
         
        test3()
    
    