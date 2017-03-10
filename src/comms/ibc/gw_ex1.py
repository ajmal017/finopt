#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep, strftime
import logging
import json

from ib.ext.Contract import Contract

from misc2.helpers import ContractHelper
from comms.ibgw.base_messaging import Prosumer
from comms.tws_protocol_helper import TWS_Protocol
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener

         
class MessageListener(AbstractGatewayListener):   
    def __init__(self, name, parent):
        AbstractGatewayListener.__init__(self, name)
        self.parent = parent

    def position(self, event, message_value):  # account, contract, pos, avgCost):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
   
    def positionEnd(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
        #self.parent.stop_manager()
        
    def error(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))  


    def gw_subscriptions(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
        

    def gw_subscription_changed(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
        

    


def test_client(kwargs):

    cm = TWS_client_manager(kwargs)
    cl = MessageListener('gw_client_message_listener', cm)
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
    cm.reqPositions()
    cm.gw_req_subscriptions()
    try:
        logging.info('TWS_gateway:main_loop ***** accepting console input...')
        while not cm.is_stopped(): 
        
            sleep(.45)
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('TWS_client_manager: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop()
        
        logging.info('TWS_client_manager: Service shut down complete...')
           
    print 'end of test_client function'
      
if __name__ == '__main__':
    

    
    kwargs = {
      'name': 'tws_example1',
      'bootstrap_host': 'localhost',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'localhost',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'TWS_CLI_EX1',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO},
      'topics': ['position', 'positionEnd', 'gw_subscriptions', 'gw_subscription_changed']
      }

   
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    test_client(kwargs)
    
     
