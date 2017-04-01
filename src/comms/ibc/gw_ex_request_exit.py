#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep, strftime
import logging
import json
import sys

from ib.ext.Contract import Contract
from optparse import OptionParser
from misc2.helpers import ContractHelper, HelperFunctions, ExecutionFilter
from comms.ibgw.base_messaging import Prosumer
from comms.tws_protocol_helper import TWS_Protocol
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener
from QuantLib._QuantLib import VanillaOption_priceCurve
from rethink.tick_datastore import TickDataStore
from finopt.instrument import Symbol

         
class ClientMessageListener(AbstractGatewayListener):   
    def __init__(self, name, tick_ds):
        AbstractGatewayListener.__init__(self, name)
        self.tick_ds = tick_ds

   
    def raw_dump(self, event, items):
        del(items['self'])
        logging.info('%s [[ %s ]]' % (event, items))        
   
    def position(self, event, account, contract_key, position, average_cost, end_batch):
        self.raw_dump(event, vars())

   
    def positionEnd(self, event): #, message_value):
        """ generated source for method positionEnd """
        logging.info('%s [[ %s ]]' % (event, vars()))
    
            
    def error(self, event, id, errorCode, errorMsg):
        logging.info('MessageListener:%s. val->[%s]' % (event, vars()))  

#     def update_portfolio_account(self, event, **items):
#         logging.info('MessageListener:%s. val->[%s]' % (event, items))
        
        
    '''
        the 4 account functions below are invoked by AbstractListener.update_portfolio_account.
        the original message from TWS is first wrapped into update_portfolio_account event in 
        class TWS_event_handler and then expanded by AbstractListener.update_portfolio_account
        (check tws_event_hander)
    '''
    def updateAccountValue(self, event, key, value, currency, account):  # key, value, currency, accountName):
        self.raw_dump(event, vars())
 
    def updatePortfolio(self, event, contract_key, position, market_price, market_value, average_cost, unrealized_PNL, realized_PNL, account):
        self.raw_dump(event, vars())
        
            
    def updateAccountTime(self, event, timestamp):
        self.raw_dump(event, vars())
        
    def accountDownloadEnd(self, event, account):  # accountName):
        self.raw_dump(event, vars())
        

    def tickPrice(self, event, contract_key, field, price, canAutoExecute):
        #logging.info('MessageListener:%s. %s %d %8.2f' % (event, contract_key, field, price))
        self.tick_ds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)

    def tickSize(self, event, contract_key, field, size):
        self.tick_ds.set_symbol_tick_price(contract_key, field, size, 0)
        #logging.info('MessageListener:%s. %s: %d %8.2f' % (event, contract_key, field, size))
        

    def execDetails(self, event, req_id, contract, execution, end_batch):
        self.raw_dump(event, vars())


def test_client(kwargs):

    ts = TickDataStore(kwargs['name'])
    cm = TWS_client_manager(kwargs)
    cl = ClientMessageListener('gw_client_message_listener', ts)
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
    contractTuples = [('HSI', 'FUT', 'HKFE', 'HKD', '20170330', 0, ''),
                      ('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0, ''),
                      ('AUD', 'CASH', 'IDEALPRO', 'USD', '', 0, ''),
                      ('QQQ', 'STK', 'SMART', 'USD', '', 0, ''),
                      ('YM', 'IND', 'ECBOT', 'USD', '', 0, ''),
                      ]
                          
                              
    map(lambda x: cm.reqMktData(ContractHelper.makeContract(x), False), contractTuples)
    syms = map(lambda x: Symbol(ContractHelper.makeContract(x)), contractTuples)
    map(lambda x: ts.add_symbol(x), syms)
    #cm.reqPositions()
    #cm.reqMktData(ContractHelper.makeContract(contractTuples[1]), False)
    try:
        logging.info('TWS_gateway:main_loop ***** accepting console input...')
        while not cm.is_stopped(): 
        
            sleep(.45)
            read_ch = raw_input("Enter command:")
            ts.dump()
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('TWS_client_manager: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop()
        
        logging.info('TWS_client_manager: Service shut down complete...')
           
    print 'end of test_client function'


def test_client2(kwargs):

    ts = TickDataStore(kwargs['name'])
    cm = TWS_client_manager(kwargs)
    cl = ClientMessageListener('gw_client_message_listener', ts)
    
    cm.add_listener_topics(cl, kwargs['topics'])
    cm.start_manager()
                          
                              
    #cm.reqPositions()
    #cm.reqAccountUpdates(True, 'U8379890')
    



    try:
        logging.info('gw_ex_request:main_loop ***** accepting console input...')
        menu = {}
        menu['1']="Request positions" 
        menu['2']="Request account updates "
        menu['3']="End request account updates"
        menu['4']="Request executions"
        menu['9']="Exit"
        while True: 
            choices=menu.keys()
            choices.sort()
            for entry in choices: 
                print entry, menu[entry]            

            selection = raw_input("Enter command:")
            if selection =='1':
                cm.reqPositions()
            elif selection == '2': 
                cm.reqAccountUpdates(True, 'U8379890')
            elif selection == '3':
                cm.reqAccountUpdates(False, 'U8379890')
            elif selection == '4':
                
                cm.reqExecutions()
                
            elif selection == '9': 
                cm.gw_message_handler.set_stop()
                break
            else: 
                print "Unknown Option Selected!"                 
            
            sleep(0.15)
        
    except (KeyboardInterrupt, SystemExit):
        logging.error('AnalyticsEngine: caught user interrupt. Shutting down...')
        cm.gw_message_handler.set_stop() 
        logging.info('AnalyticsEngine: Service shut down complete...')     




        
        logging.info('TWS_client_manager: Service shut down complete...')
           
    print 'end of test_client function'
   
if __name__ == '__main__':
    

    
    kwargs = {
      'name': 'simple_request',
      'bootstrap_host': 'localhost',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'localhost',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'EX_REQUEST',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/gw_ex.log'},
      #'topics': ['tickSize', 'tickPrice',  'position', 'positionEnd', 'updateAccountValue', 'updatePortfolio', 'updateAccountTime', 'accountDownloadEnd'],
      'topics': ['tickSize', 'tickPrice',  'position', 'update_portfolio_account', 'execDetails'],
      'seek_to_end': ['tickPrice', 'tickSize']
      }

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--clear_offsets", action="store_true", dest="clear_offsets",
                      help="delete all redis offsets used by this program")
    parser.add_option("-g", "--group_id",
                      action="store", dest="group_id", 
                      help="assign group_id to this running instance")
    parser.add_option("-n", "--name",
                      action="store", dest="name", 
                      help="assign an identifier to this running instance")
    
    
    
    (options, args) = parser.parse_args()
    if options.name == None or options.group_id == None:
        print "Name or Group id was not specified. Use -h to see all options. Exiting..."
        sys.exit()
        
    for option, value in options.__dict__.iteritems():
        if value <> None:
            kwargs[option] = value
            
    print kwargs    
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    test_client2(kwargs)

    
     