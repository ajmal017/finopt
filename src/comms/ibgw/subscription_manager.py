#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import strftime
import json
from misc2.helpers import ContractHelper
from ib.ext.Contract import Contract
from comms.ibgw.base_messaging import BaseMessageListener



class SubscriptionManager(BaseMessageListener):
    
    
    
    
    def __init__(self, name, tws_connection, producer, rs_conn, subscription_key):
        BaseMessageListener.__init__(self, name)
        
        self.tws_connect = tws_connection
        self.producer = producer
        self.rs = rs_conn
        self.subscription_key = subscription_key

        
        self.handle = []
        # contract key map to contract ID (index of the handle array)
        self.tickerId = {}   
        # flag to indicate whether to save changes when persist_subscriptions is called       
        self.is_dirty = False

        self.load_subscriptions()
        
            

        
        
    def load_subscriptions(self):
        
        def is_outstanding(c):
            
            today = strftime('%Y%m%d') 
            if c.m_expiry < today and (c.m_secType == 'OPT' or c.m_secType == 'FUT'):
                logging.info('initialize_subscription_mgr: ignoring expired contract %s%s%s' % (c.m_expiry, c.m_strike, c.m_right))
                return False
            return True
            
        if self.rs.get(self.subscription_key):
            contracts = filter(lambda x: is_outstanding(x), 
                                   map(lambda x: ContractHelper.kvstring2object(x, Contract), json.loads(self.rs.get(self.subscription_key))))        
            for c in contracts:
                logging.info('SubscriptionManager:load_subscription. request market data for: %s' % (ContractHelper.printContract(c)))
                self.reqMktData('internal-dummy-call', {'value': ContractHelper.contract2kvstring(c)}) 
            
            
        
    
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
            contract.m_conId = 0
            self.tws_connect.reqMktData(id, contract, '', False) 
            self.is_dirty = True
                
            logging.info('SubscriptionManager:reqMktData. Requesting market data, id = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        else:    
            self.tws_connect.reqMktData(1000 + id, contract, '', True)
            logging.info('SubscriptionManager:reqMktData. contract already subscribed. Request snapshot = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        #self.dump()

        #
        # instruct gateway to broadcast new id has been assigned to a new contract
        #
        self.producer.send_message('gw_subscription_changed', self.producer.message_dumps({id: ContractHelper.object2kvstring(contract)}))
        logging.info('SubscriptionManager:reqMktData. Publish gw_subscription_changed: %d:%s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        
        

    # use only after a broken connection is restored
    # to re request market data 
    #>>>> not enhanced yet...old code
    def force_resubscription(self):
        # starting from index 1 of the contract list, call  reqmktdata, and format the result into a list of tuples
        for i in range(1, len(self.handle)):
            self.tws_connect.reqMktData(i, self.handle[i], '', False)
            logging.info('force_resubscription: %s' % ContractHelper.printContract(self.handle[i]))
       
            
    def itemAt(self, id):
        if id > 0 and id < len(self.handle):
            return self.handle[id]
        return -1


    def persist_subscriptions(self):
         
        if self.is_dirty:
            cs = json.dumps(map(lambda x: ContractHelper.object2kvstring(x) if x <> None else None, self.handle))
            logging.info('Tws_gateway:persist_subscriptions. updating subscription table to redis store %s' % cs)
            self.dump()
            self.rs.set(self.subscription_key, cs)
            self.is_dirty = False


    def dump(self):
        
        logging.info('subscription manager table:---------------------\n')
        logging.info(''.join('%d: {%s},\n' % (i,  ''.join('%s:%s, ' % (k, v) for k, v in self.handle[i].__dict__.iteritems() )\
                                     if self.handle[i] <> None else ''          ) for i in range(len(self.handle)))\
                     )
        
        #logging.info( ''.join('%s[%d],\n' % (k, v) for k, v in self.conId.iteritems()))
        logging.info( 'Number of instruments subscribed: %d' % len(self.handle))
        logging.info( '------------------------------------------------')


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
    
