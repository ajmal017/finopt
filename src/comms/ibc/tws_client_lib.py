#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import copy
from time import sleep, strftime

import logging
import json

from ib.ext.Contract import Contract

from misc2.helpers import ContractHelper, ExecutionFilterHelper, OrderHelper, ConfigMap
from comms.ibgw.base_messaging import Prosumer, BaseMessageListener
from comms.tws_protocol_helper import TWS_Protocol
from misc2.observer import NotImplementedException
from comms.ibc.base_client_messaging import GatewayCommandWrapper, AbstractGatewayListener
import redis
         
         
         
class TWS_client_manager(GatewayCommandWrapper):

    
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
        

        temp_kwargs = copy.copy(kwargs)
        self.kwargs = copy.copy(TWS_client_manager.TWS_CLI_DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in temp_kwargs:
                self.kwargs[key] = temp_kwargs.pop(key)        
        self.kwargs.update(temp_kwargs)        
        
        '''
            TWS_client_manager start up sequence
            
            1. establish redis connection
            2. initialize prosumer instance - gateway message handler
            
            4. initialize listeners: 
            5. start the prosumer 
        
        '''

        logging.info('starting up TWS_client_manager...')
        logging.info('establishing redis connection...')
        self.initialize_redis()
        
        logging.info('starting up gateway message handler - kafka Prosumer...')        
        self.gw_message_handler = Prosumer(name='tws_cli_prosumer', kwargs=self.kwargs)
        GatewayCommandWrapper.__init__(self, self.gw_message_handler)        



                
                
                    
        
        logging.info('**** Completed initialization sequence. ****')
        
    def is_stopped(self):
        return self.gw_message_handler.is_stopped()
    
    def stop_manager(self):
        self.gw_message_handler.set_stop()

    def start_manager(self):
        logging.info('start gw_message_handler. Entering processing loop...')
        self.gw_message_handler.start_prosumer()
    
    def add_listener_topics(self, listener, topics):
        self.gw_message_handler.add_listener_topics(listener, topics)

    def initialize_redis(self):

        self.rs = redis.Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        try:
            self.rs.client_list()
        except redis.ConnectionError:
            logging.error('TWS_client_manager: unable to connect to redis server using these settings: %s port:%d db:%d' % 
                          (self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db']))
            logging.error('aborting...')
            sys.exit(-1)
        

                                  
        
        


    

       
class GatewayMessageListener(AbstractGatewayListener):   
    def __init__(self, name):
        AbstractGatewayListener.__init__(self, name)
             
    def tickPrice(self, event, message_value):  # tickerId, field, price, canAutoExecute):
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, message_value))

    def tickSize(self, event, message_value):  # tickerId, field, price, canAutoExecute):
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, message_value))
        
    def error(self, event, message_value):
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, message_value))  

def test_client(kwargs):
    contractTuples = [('HSI', 'FUT', 'HKFE', 'HKD', '20170330', 0, ''),
                      ('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0, ''),]
                      
        
    print kwargs 
    cm = TWS_client_manager(kwargs)
    cl = GatewayMessageListener('gw_client_message_listener')
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
    map(lambda c: cm.reqMktData(ContractHelper.makeContract(c)), contractTuples)
    try:
        logging.info('TWS_gateway:main_loop ***** accepting console input...')
        while True: 
        
            sleep(.45)
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('TWS_client_manager: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop()
        
        logging.info('TWS_client_manager: Service shut down complete...')
           
        
if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    



    cfg_path= sys.argv[1:]
    kwargs = ConfigMap().kwargs_from_file(cfg_path)
   
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    test_client(kwargs)
    
     
