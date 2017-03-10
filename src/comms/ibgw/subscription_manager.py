#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from misc2.helpers import ContractHelper
from ib.ext.Contract import Contract
from comms.ibgw.base_messaging import BaseMessageListener



class SubscriptionManager(BaseMessageListener):
    
    
    persist_f = None
    
    def __init__(self, name, tws_gateway):
        BaseMessageListener.__init__(self, name)
        self.tws_connect = tws_gateway.tws_connection
        self.producer = tws_gateway.gw_message_handler
        self.handle = []    
        # contract key map to contract ID (index of the handle array)
        self.tickerId = {}
   
        
        
    def load_subscription(self, contracts):
        for c in contracts:
            #print self.tws_connect.isConnected() 
            print '%s' % (ContractHelper.printContract(c))
            self.reqMktData('internal', {'value': ContractHelper.contract2kvstring(c)}) 
            
            
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
    

            
    def reqMktData(self, event, message):
                  
        contract = ContractHelper.kvstring2object(message['value'], Contract)
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
        self.producer.send_message('gw_subscription_changed', self.producer.message_dumps({id: ContractHelper.object2kvstring(contract)}))
        #>>>self.parent.gw_notify_subscription_changed({id: ContractHelper.object2kvstring(contract)})
        logging.info('SubscriptionManager reqMktData: gw_subscription_changed: %d:%s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
    """
       Client requests to TWS_gateway
    """
    def gw_req_subscriptions(self, event, message):
        
        #subm = map(lambda i: ContractHelper.contract2kvstring(self.contract_subscription_mgr.handle[i]), range(len(self.contract_subscription_mgr.handle)))
        #subm = map(lambda i: ContractHelper.object2kvstring(self.contract_subscription_mgr.handle[i]), range(len(self.contract_subscription_mgr.handle)))
        subm = map(lambda i: (i, ContractHelper.object2kvstring(self.handle[i])),
                    range(len(self.handle)))
        
        
        if subm:
            
            logging.info('SubscriptionManager:gw_req_subscriptions-------\n%s' % ''.join('\n%s:%s' % (str(v[0]).rjust(6), v[1]) for v in subm))
            self.producer.send_message('gw_subscriptions', self.producer.message_dumps({'subscriptions': subm}))
        
        

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
    
