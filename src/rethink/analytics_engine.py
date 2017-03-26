import logging
import json
import copy
from optparse import OptionParser
from time import sleep
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from finopt.instrument import Symbol
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener
from comms.ibgw.base_messaging import BaseMessageListener




class AnalyticsEngine(AbstractGatewayListener):

    AE_OPTIONS_CONFIG = {
        'underlying_substitution': {'IND': 'FUT'},
        'underlying_sub_list': ['HSI', 'MHI']
    }
    
    
    
    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
 
        self.option_chains = {}
        
    
    def test_oc(self, oc2):
        expiry = '20170330'
        contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '', 0, expiry)
        contract = ContractHelper.makeContract(contractTuple)  
        
        oc2.set_option_structure(contract, 200, 50, 0.0012, 0.0328, expiry)        
        
        oc2.build_chain(24119, 0.03, 0.22)
        
#         expiry='20170324'
#         contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
#         contract = ContractHelper.makeContract(contractTuple)  
# 
#         oc2.set_option_structure(contract, 0.5, 100, 0.0012, 0.0328, expiry)        
#     
#         oc2.build_chain(132.11, 0.02, 0.22)
        
        
        oc2.pretty_print()        

        for o in oc2.get_option_chain():
            self.tds.add_symbol(o)
        self.tds.add_symbol(oc2.get_underlying())
        
    
    def start_engine(self):
        self.twsc.start_manager()
        oc2 = OptionsChain('oc2')
        oc2.register_listener(self)
        
        self.test_oc(oc2)
        
        try:
            logging.info('AnalyticsEngine:main_loop ***** accepting console input...')
            while True: 
            

                read_ch = raw_input("Enter command:")
                oc2.pretty_print()
                sleep(0.45)
            
        except (KeyboardInterrupt, SystemExit):
            logging.error('AnalyticsEngine: caught user interrupt. Shutting down...')
            self.twsc.gw_message_handler.set_stop()
            
            logging.info('AnalyticsEngine: Service shut down complete...')               
    
    
    #         EVENT_OPTION_UPDATED = 'oc_option_updated'
    #         EVENT_UNDERLYING_ADDED = 'oc_underlying_added
    def oc_option_updated(self, event, update_mode, name, instrument):        
        logging.info('oc_option_updated. %s %s' % (event, vars()))
        self.tds.add_symbol(instrument)
        self.twsc.reqMktData(instrument.get_contract(), True)
        
    
    def oc_underlying_added(self, event, update_mode, name, instrument):
        
        logging.info('oc_underlying_added. %s %s' % (event, vars()))
        self.tds.add_symbol(instrument)
        self.twsc.reqMktData(instrument.get_contract(), True)

    #
    # tds call backs
    #
    #     
    #         EVENT_TICK_UPDATED = 'tds_event_tick_updated'
    #         EVENT_SYMBOL_ADDED = 'tds_event_symbol_added'
    #         EVENT_SYMBOL_DELETED = 'tds_event_symbol_deleted'    
    
    def tds_event_symbol_added(self, event, update_mode, name, instrument):
       pass
        #logging.info('tds_event_new_symbol_added. %s' % ContractHelper.object2kvstring(symbol.get_contract()))
        
    
    def tds_event_tick_updated(self, event, contract_key, field, price, canAutoExecute):
        #tds_event_tick_updated:
        # dict object: {'partition': 0, 'value': '{"field": 7, "price": 35.0, "canAutoExecute": 0, "tickerId": 10}', 'offset': 527}
        #logging.info('tds_event_tick_updated. %s' % items)
        pass

    def tds_event_symbol_deleted(self, event, update_mode, name, instrument):
        pass
    #
    # external ae requests
    #
    def ae_req_greeks(self, event, message_value):
        #(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) 
        pass
    
    def ae_req_tds_internal(self, event, message_value):
        logging.info('received ae_req_tds_internal')
        self.tds.dump()
    
    #
    # gateway events
    #

    def tickPrice(self, event, contract_key, field, price, canAutoExecute):
        logging.info('MessageListener:%s. %s %d %8.2f' % (event, contract_key, field, price))
        self.tds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)

    def tickSize(self, event, contract_key, field, size):
        self.tds.set_symbol_tick_price(contract_key, field, size, 0)
        #logging.info('MessageListener:%s. %s: %d %8.2f' % (event, contract_key, field, size))
 
    def error(self, event, message_value):
        logging.info('AnalyticsEngine:%s. val->[%s]' % (event, message_value))         
        
        
if __name__ == '__main__':
    

    
    kwargs = {
      'name': 'analytics_engine',
      'bootstrap_host': 'localhost',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'localhost',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'AE',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/ae.log'},
      'topics': ['tickPrice'],
      'seek_to_end': ['*'],
      #'seek_to_end':['tickSize', 'tickPrice','gw_subscriptions', 'gw_subscription_changed']
      }

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--clear_offsets", action="store_true", dest="clear_offsets",
                      help="delete all redis offsets used by this program")
    parser.add_option("-g", "--group_id",
                      action="store", dest="group_id", 
                      help="assign group_id to this running instance")
    
    (options, args) = parser.parse_args()
    for option, value in options.__dict__.iteritems():
        if value <> None:
            kwargs[option] = value
            
  
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    server = AnalyticsEngine(kwargs)
    server.start_engine()
    
          
        