#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import strftime
import json
from misc2.helpers import ContractHelper
from ib.ext.Contract import Contract
from comms.ibgw.base_messaging import BaseMessageListener
from comms.ibgw.tws_event_handler import TWS_event_handler



class SubscriptionManager(BaseMessageListener):
    
    
    TICKER_GAP = 1000
    
    def __init__(self, name, tws_connection, producer, rs_conn, subscription_key):
        BaseMessageListener.__init__(self, name)
        
        self.tws_connect = tws_connection
        self.producer = producer
        self.rs = rs_conn
        self.subscription_key = subscription_key

        #self.handle = []
        # contract key map to contract ID (index of the handle array)
        #self.tickerId = {}
        '''
            idContractMap has 3 keys
            
            next_id keeps track of the next_id to use when subscribing market data from TWS
            id_contract and contract_id are dict and reverse dict that store the index of id 
            to contarct and vice versa
            
            id_contract: {<int>, <Contract>}
            contract_id: {<kvs_contract>, <int>}
            
        '''
        self.idContractMap ={'next_id': 0, 'id_contract':{},'contract_id':{}}       
        # flag to indicate whether to save changes when persist_subscriptions is called       
        self.is_dirty = False

        self.load_subscriptions()
        
            

        
        
    def load_subscriptions(self):
        
        def is_outstanding(ic):
            
            c = ic[1]
            today = strftime('%Y%m%d') 
            if c.m_expiry < today and (c.m_secType == 'OPT' or c.m_secType == 'FUT'):
                logging.info('initialize_subscription_mgr: ignoring expired contract %s%s%s' % (c.m_expiry, c.m_strike, c.m_right))
                return False
            return True
            
        # retrieve the id-contract list from db
        # remap the list by instantiating the string to object
        # get rid of the already expired contracts
        saved_iclist = self.get_id_contracts(db=True)
        if saved_iclist:
            ic_list= filter(lambda ic:is_outstanding, map(lambda ic: (ic[0], ContractHelper.kvstring2object(ic[1], Contract)), saved_iclist))
            
            map(lambda ic: self.request_market_data(ic[0], ic[1], snapshot=False), ic_list) 
            map(lambda ic: self.request_market_data(ic[0], ic[1], snapshot=True), ic_list)         
            logging.info('SubscriptionManager:load_subscription. request market data for: %s' % (ic_list))
        
    
    def request_market_data(self, id, contract, snapshot=False):
        if snapshot:
            # the call to TWS will return a snapshot follow 
            # by the subscription being cancelled. Add 1000 to avoid clashing 
            # with other subscription ids.  
            self.tws_connect.reqMktData(id + TWS_event_handler.TICKER_GAP, contract, '', True)
        else:
            self.tws_connect.reqMktData(id, contract, '', False)
            
    
    # returns -1 if not found, else the key id (which could be a zero value)
    def is_subscribed(self, contract):

        
        ckey = ContractHelper.makeRedisKeyEx(contract)
        try:
            return self.idContractMap['contract_id'][ckey]
        except KeyError:
            return -1

    def add_subscription(self, contract):
        #
        # structure of idContractMap ={'next_id': -1, 'id_contract':{}, 'contract_id':{}}
        #
        id = self.idContractMap['next_id']
        self.idContractMap['id_contract'][id] = contract
        self.idContractMap['contract_id']['ContractHelper.makeRedisKeyEx(contract)'] = id        
        self.idContractMap['next_id'] = id + 1
  
        return self.idContractMap['next_id']

            
    def reqMktData(self, event, message):
                  
        contract = ContractHelper.kvstring2object(message['value'], Contract)
        #logging.info('SubscriptionManager: reqMktData')
  
        id = self.is_subscribed(contract)
        if id == -1: # not found
            id = self.add_subscription(contract)
            #
            # the conId must be set to zero when calling TWS reqMktData
            # otherwise TWS will fail to subscribe the contract
            contract.m_conId = 0
            self.request_market_data(id, contract, False) 
            self.is_dirty = True
                
            logging.info('SubscriptionManager:reqMktData. Requesting market data, id = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        else:    
            self.request_market_data(id, contract, True)
            logging.info('SubscriptionManager:reqMktData. contract already subscribed. Request snapshot = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        #self.dump()

        #
        # instruct gateway to broadcast new id has been assigned to a new contract
        #
        self.producer.send_message('gw_subscription_changed', self.producer.message_dumps({id: ContractHelper.object2kvstring(contract)}))
        logging.info('SubscriptionManager:reqMktData. Publish gw_subscription_changed: %d:%s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        
        

    # use only after a broken connection is restored
    def force_resubscription(self):
       self.load_subscriptions()
            

    # return id:contract object
    def get_id_contracts(self, db=False):
        if db:
            try:
                id_contracts = json.loads(self.rs.get(self.subscription_key))
                return map(lambda x: (x[0], ContractHelper.kv2object(x[1], Contract), id_contracts))
            except TypeError:
                logging.info('SubscriptionManager:get_id_contracts. Exception when trying to get id_contracts from redis ***')
                return None
        else:
            return map(lambda x: (x[0], x[1]), 
                                list(self.idContractMap['id_contract'].iteritems()))

    # return id:contract_strings
    def get_id_kvs_contracts(self, db):
        return map(lambda x:(x[0], ContractHelper.contract2kvstring(x[1])), self.get_id_contracts(db))
    
    def persist_subscriptions(self):
         
#         if self.is_dirty:
#             cs = json.dumps(map(lambda x: ContractHelper.object2kvstring(x) if x <> None else None, self.handle))
#             logging.info('Tws_gateway:persist_subscriptions. updating subscription table to redis store %s' % cs)
#             self.dump()
#             self.rs.set(self.subscription_key, cs)
#             self.is_dirty = False

        #self.idContractMap ={'next_id': -1, 'id_contract':{}, 'contract_id':{}}
        if self.is_dirty:
            # for each id:contract pair in idContractMap['id_contract'] dict, map to a list of (id, kvs_contract) values
            ic = json.dumps(self.get_id_kvs_contracts(db=False))
            self.rs.set(self.subscription_key, ic)
            self.is_dirty = False

            logging.info('Tws_gateway:persist_subscriptions. updating subscription table to redis store %s' % ic)
            self.dump()

    def dump(self):

        logging.info('subscription manager table:---------------------\n')
        logging.info(''.join ('\n[%s]:[%s]' % (str(ic[0]).rjust(4), ic[1]) for ic in self.get_id_kvs_contracts(db=False)))
        logging.info(''.join ('\n[%s]:[%d]' % (k.rjust(20), self.idContractMap['contract_id'][k]) 
                               for k in sorted(self.idContractMap['contract_id'])))       
        logging.info( 'Number of instruments subscribed: %d' % self.idContractMap['next_id'])
        logging.info( '------------------------------------------------')


    """
       Client requests to TWS_gateway
    """
    def gw_req_subscriptions(self, event, message):
        
        ic = self.get_id_kvs_contracts(db=False)
        if ic:
             
            logging.info('SubscriptionManager:gw_req_subscriptions-------\n%s' % ic)
            self.producer.send_message('gw_subscriptions', self.producer.message_dumps({'subscriptions': ic}))
        
       


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
    
