#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import copy
from time import sleep, strftime
import ConfigParser
import logging
import json

from ib.ext.Contract import Contract
from ib.ext.EClientSocket import EClientSocket

from misc2.helpers import ContractHelper
from comms.ibgw.base_messaging import Prosumer
from comms.ibgw.tws_event_handler import TWS_event_handler
from comms.ibgw.client_request_handler import ClientRequestHandler
from comms.ibgw.subscription_manager import SubscriptionManager
from comms.tws_protocol_helper import TWS_Protocol 
import redis
         
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
      'tws_app_id': 38888,
      'group_id': 'TWS_GW',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'order_transmit': False,
      'topics': list(TWS_Protocol.topicMethods) + list(TWS_Protocol.gatewayMethods)
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
                
                today = strftime('%Y%m%d') 
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
        if type(self.kwargs['tws_app_id']) <> int:
            logging.error('TWS_gateway:connect_tws. tws_app_id must be of int type but detected %s!' % str(type(kwargs['tws_app_id'])))
            sys.exit(-1)
            
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
                
                sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('TWS_gateway: caught user interrupt. Shutting down...')
                self.gw_message_handler.set_stop()
                self.gw_message_handler.join()
                logging.info('TWS_gateway: Service shut down complete...')
                sys.exit(0)        



    
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
    
     
