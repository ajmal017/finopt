#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import copy
from time import sleep, strftime
import logging
import json
from optparse import OptionParser
from ib.ext.Contract import Contract

from misc2.helpers import ContractHelper, ExecutionFilterHelper, OrderHelper, ConfigMap
from comms.ibgw.base_messaging import Prosumer, BaseMessageListener
from comms.ibc.base_client_messaging import GatewayCommandWrapper, AbstractGatewayListener
from comms.tws_protocol_helper import TWS_Protocol
from misc2.observer import NotImplementedException
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
      
      'topics': list(TWS_Protocol.topicEvents) 
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
        self.gw_message_handler = Prosumer(name=self.kwargs['name'], kwargs=self.kwargs)
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
             
    #def tickPrice(self, event, message_value):  # tickerId, field, price, canAutoExecute):
    def tickPrice(self, event, contract_key, field, price, canAutoExecute):  # tickerId, field, price, canAutoExecute):    
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, vars()))

    #tickSize(self, event, contract_key, tickerId, field, size):
    #def tickSize(self, event, message_value):  # tickerId, field, price, canAutoExecute):
    def tickSize(self, event, contract_key, field, size):
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, vars()))
        
    def error(self, event, message_value):
        logging.info('GatewayMessageListener:%s. val->[%s]' % (event, message_value))  

def test_client(kwargs):
    contractTuples = [('HSI', 'FUT', 'HKFE', 'HKD', '20190130', 0, '')]#,
    #contractTuples = [('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0, ''),]
                      
        
    print kwargs 
    cm = TWS_client_manager(kwargs)
    cl = GatewayMessageListener('gw_client_message_listener')
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
    map(lambda c: cm.reqMktData(ContractHelper.makeContract(c)), contractTuples)
    cm.reqPositions()
    try:
        logging.info('TWS_gateway:main_loop ***** accepting console input...')
        while True: 
        
            sleep(.45)
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('TWS_client_manager: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop()
        
        logging.info('TWS_client_manager: Service shut down complete...')
           
        
if __name__ == '__main__':
    
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--clear_offsets", action="store_true", dest="clear_offsets",
                      help="delete all redis offsets used by this program")
    parser.add_option("-f", "--config_file",
                      action="store", dest="config_file", 
                      help="path to the config file")
    
    (options, args) = parser.parse_args()
    
    kwargs = ConfigMap().kwargs_from_file(options.config_file)
    for option, value in options.__dict__.iteritems():
        
        if value <> None:
            kwargs[option] = value


    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        

    logging.debug('config settings: %s' % kwargs)
    
    
    test_client(kwargs)
    
     
