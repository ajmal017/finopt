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
    
    def __init__(self, name, tws_connection, producer, rs_conn, kwargs):
        BaseMessageListener.__init__(self, name)
        
        self.tws_connect = tws_connection
        self.producer = producer
        self.rs = rs_conn
        self.subscription_key = kwargs['subscription_manager.subscriptions.redis_key']
        
        
        self.reset_subscriptions(kwargs['reset_db_subscriptions'])

        #self.handle = []
        # contract key map to contract ID (index of the handle array)
        #self.tickerId = {}
        '''
            idContractMap has 3 keys
            
            next_id keeps track of the next_id to use when subscribing market data from TWS
            id_contract and contract_id are dict and reverse dict that store the index of id 
            to contarct and vice versa
            
            id_contract: {<int>, <contract-redis-key>}
            contract_id: {<contract-redis-key>, <int>}
            
        '''
        self.idContractMap ={'next_id': 0, 'id_contract':{},'contract_id':{}}       
        # flag to indicate whether to save changes when persist_subscriptions is called       
        self.is_dirty = False

        #logging.warn('***** TEMPORARILY skip loading subscriptions from redis!!!')
        self.load_subscriptions()
        
    
    def get_contract_by_id(self, id):
        
        try:
            logging.debug('get_contract_by_id %d' % id)
            #self.dump()
            return self.idContractMap['id_contract'][id]
                
        except (KeyError, ):
            
            if (id  >= TWS_event_handler.TICKER_GAP):
                return self.idContractMap['id_contract'][id  - TWS_event_handler.TICKER_GAP]

        
        raise
            
    def reset_subscriptions(self, reset_db):
        if reset_db:
            logging.warn('SubscriptionManager:reset_subscriptions. Delete subscription entry in redis')
            self.rs.delete(self.subscription_key)
        
    def load_subscriptions(self):
        '''
            the function retrieves a json string representation of a list of {id:contracts}
            from redis.
            next it gets rid of fut and opt contracts that have expired
            next, it rebuilds the internal dict idContractMap['id_contract'] and reverse dict
            idContractMap['contract_id']
            it then gathers all the ids in the newly populated dict (which may contain holes due to
            expired contracts) and determine the max id
            add 1 to it to get the next_id
            request snapshot and new market data from the TWS gateway
            
        '''
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
            
            ic_list= filter(lambda ic:is_outstanding, saved_iclist)
            # rebuild the internal data map
            for ic in ic_list:
                self.idContractMap['id_contract'][ic[0]] = ic[1]
                self.idContractMap['contract_id'][ic[1]] = ic[0]        
            
            # derive the next id by finding the max id
            max_id = reduce(lambda x,y: max(x,y), self.idContractMap['id_contract'].keys())
            self.idContractMap['next_id'] = max_id + 1
            logging.info('SubscriptionManager:load_subscription. the next_id is set to: %d' % (self.idContractMap['next_id']))
            self.dump()
            # subscribe market data, first call is normal subscription,
            # first for snapshot, then subscribe for the latest
            logging.info('SubscriptionManager:load_subscription. request market data for: %s' % (ic_list))
            
            map(lambda ic: self.request_market_data(ic[0], ContractHelper.makeContractfromRedisKeyEx(ic[1]), snapshot=True), ic_list)
            map(lambda ic: self.request_market_data(ic[0], ContractHelper.makeContractfromRedisKeyEx(ic[1]), snapshot=False), ic_list)
            
        else:
            logging.warn('SubscriptionManager:load_subscription. No saved id:contracts found in redis.')
             
        logging.info('SubscriptionManager:load_subscription. Complete populating stored map into idContract dict.')
    
    def request_market_data(self, id, contract, snapshot=False):
        contract.m_conId = 0
        if snapshot:
            # the call to TWS will return a snapshot follow 
            # by the subscription being cancelled. Add 1000 to avoid clashing 
            # with other subscription ids.  
            logging.info( 'request_market_data: %d %s' % (id + TWS_event_handler.TICKER_GAP, ContractHelper.printContract(contract)))
            self.tws_connect.reqMktData(id + TWS_event_handler.TICKER_GAP, contract, '', True)
            
        else:
            self.tws_connect.reqMktData(id, contract, '', False)
#
#         self.tws_connect.reqMktData(id + TWS_event_handler.TICKER_GAP, contract, '', True)
#         self.tws_connect.reqMktData(id, contract, '', False)
    
    # returns -1 if not found, else the key id (which could be a zero value)
    def is_subscribed(self, contract):

        
        ckey = ContractHelper.makeRedisKeyEx(contract)
        logging.debug('is_subscribed %s' % ckey)
        try:
            return self.idContractMap['contract_id'][ckey]
        except KeyError:
            logging.warn('is_subscribed: key not found %s' % ckey)
            return -1

    def add_subscription(self, contract):
        #
        # structure of idContractMap ={'next_id': -1, 'id_contract':{}, 'contract_id':{}}
        #
        id = self.idContractMap['next_id']
        key = ContractHelper.makeRedisKeyEx(contract)
        self.idContractMap['id_contract'][id] = key
        logging.info('add_subscription %s' % key)
        self.idContractMap['contract_id'][key] = id        
        self.idContractMap['next_id'] = id + 1
  
        return id

            
    '''
     this function gets called whenever a client request for market data
     check the line below at tws_gateway __init__
     
         self.gw_message_handler.add_listener_topics(self.contract_subscription_mgr, self.kwargs['subscription_manager.topics'])
         
    ''' 
    def reqMktData(self, event, contract, snapshot):        
                  
#         contract = ContractHelper.kvstring2object(message['contract'], Contract)
#         snapshot = message['snapshot']
        #logging.info('SubscriptionManager: reqMktData')

        contract = ContractHelper.kvstring2object(contract, Contract)
        
        
        id = self.is_subscribed(contract)
        logging.info('reqMktData subscription_manager: id %d' % id)
        
        if id == -1: # not found
            
            id = self.add_subscription(contract)
            #
            # the conId must be set to zero when calling TWS reqMktData
            # otherwise TWS will fail to subscribe the contract
            
            self.request_market_data(id, contract, True)
            self.request_market_data(id, contract, False)
            self.is_dirty = True
                
            logging.info('SubscriptionManager:reqMktData. New request: id = %d, contract = %s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        else:  
            self.request_market_data(id, contract, True)
            logging.info('SubscriptionManager:reqMktData. Existing request get snapshot. id: %d, contract = %s snapshot=%s' % 
                         (id, ContractHelper.makeRedisKeyEx(contract), snapshot))
        #self.dump()

        #
        # instruct gateway to broadcast new id has been assigned to a new contract
        #
        '''
        sample value for gw_ga:
        {
        'partition': 0, 'value': '{"target_id": "analytics_engine", "sender_id": "tws_gateway_server", 
        "subscriptions": [[0, "{\\"m_conId\\": 0, \\"m_right\\": \\"\\", \\"m_symbol\\": \\"HSI\\", \\"m_secType\\": \\"FUT\\", 
        \\"m_includeExpired\\": false, \\"m_expiry\\": \\"20170330\\", \\"m_currency\\": \\"HKD\\", \\"m_exchange\\": \\"HKFE\\", \\"m_strike\\": 0}"]]}', 
        'offset': 13
        }
        '''      
#         subscription_array =  {'subscriptions': [[id, ContractHelper.object2kvstring(contract)]] }
#         self.producer.send_message('gw_subscription_changed', self.producer.message_dumps( subscription_array   ))
#         logging.info('SubscriptionManager:reqMktData. Publish gw_subscription_changed: %d:%s' % (id, ContractHelper.makeRedisKeyEx(contract)))
        
        
        

    # use only after a broken connection is restored
    def force_resubscription(self):
       self.load_subscriptions()
            

    # return id:contract object
    def get_id_contracts(self, db=False):
        if db:
            try:
                id_contracts = json.loads(self.rs.get(self.subscription_key))
                

                return id_contracts
            except TypeError:
                logging.error('SubscriptionManager:get_id_contracts. Exception when trying to get id_contracts from redis ***')
                return None
        else:
            return map(lambda x: (x[0], x[1]), 
                                list(self.idContractMap['id_contract'].iteritems()))

    # return id:contract_strings
    def get_id_kvs_contracts(self, db):
        return map(lambda x:(x[0], ContractHelper.contract2kvstring(x[1])), self.get_id_contracts(db))
    
    def persist_subscriptions(self):
         

        if self.is_dirty:
            # for each id:contract pair in idContractMap['id_contract'] dict, map to a list of (id, kvs_contract) values
            ic = json.dumps(self.get_id_contracts(db=False))
            self.rs.set(self.subscription_key, ic)
            self.is_dirty = False

            logging.info('Tws_gateway:persist_subscriptions. updating subscription table to redis store %s' % ic)
            self.dump()

    def dump(self):

        s = 'subscription manager table:---------------------\n'
        s = s + ''.join ('\n[%s]:[%s]' % (str(ic[0]).rjust(4), ic[1]) for ic in self.get_id_contracts(db=False))
        s = s + ''.join ('\n[%s]:[%d]' % (k.rjust(20), self.idContractMap['contract_id'][k]) 
                               for k in sorted(self.idContractMap['contract_id']))       
        s = s +  'Number of instruments subscribed: %d' % self.idContractMap['next_id']
        s = s +  '------------------------------------------------'

        logging.info(s)
        return s

#         logging.info('subscription manager table:---------------------\n')
#         logging.info(''.join ('\n[%s]:[%s]' % (str(ic[0]).rjust(4), ic[1]) for ic in self.get_id_contracts(db=False)))
#         logging.info(''.join ('\n[%s]:[%d]' % (k.rjust(20), self.idContractMap['contract_id'][k]) 
#                                for k in sorted(self.idContractMap['contract_id'])))       
#         logging.info( 'Number of instruments subscribed: %d' % self.idContractMap['next_id'])
#         logging.info( '------------------------------------------------')


    """
       Client requests to TWS_gateway
    """
    def gw_req_subscriptions(self, event, message):
        try:
            from_id = json.loads(message['value'])['sender_id']
        except:
            from_id = '<empty_sender_id>'
            
        ic = self.get_id_contracts(db=False)
    
        
        self.producer.send_message('gw_subscriptions', self.producer.message_dumps({'subscriptions': ic , 'sender_id':self.name, 'target_id':from_id}))
        logging.info('SubscriptionManager:gw_req_subscriptions-------\n%s' % self.producer.message_dumps({'subscriptions': ic , 'sender_id':self.name, 'target_id':from_id}))
       
