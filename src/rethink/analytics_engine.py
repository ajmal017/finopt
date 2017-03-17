import logging
import json
import copy
from optparse import OptionParser
from time import sleep
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from finopt.options_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener





class AnalyticsEngine(Subscriber, AbstractGatewayListener):

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
        self.request_subscrptions()
        oc2 = OptionsChain('oc2')
        self.test_oc(oc2)
        
        try:
            logging.info('AnalyticsEngine:main_loop ***** accepting console input...')
            while True: 
            

                oc2.pretty_print()
                sleep(10.0)
            
        except (KeyboardInterrupt, SystemExit):
            logging.error('AnalyticsEngine: caught user interrupt. Shutting down...')
            self.twsc.gw_message_handler.set_stop()
            
            logging.info('AnalyticsEngine: Service shut down complete...')               
    
    
    def request_subscrptions(self):
        self.initial_run = True
        self.twsc.gw_req_subscriptions(self.kwargs['name'])
        while self.initial_run:
            sleep(0.5)


    #
    # tds call backs
    #
    def tds_event_new_symbol_added(self, event, symbol):
       
        #logging.info('tds_event_new_symbol_added. %s' % ContractHelper.object2kvstring(symbol.get_contract()))
        self.twsc.reqMktData(symbol.get_contract())
    
    def tds_event_tick_updated(self, event, items):
        #tds_event_tick_updated:
        # dict object: {'partition': 0, 'value': '{"field": 7, "price": 35.0, "canAutoExecute": 0, "tickerId": 10}', 'offset': 527}
        #logging.info('tds_event_tick_updated. %s' % items)
        pass
        # this is a callback after tick price is updated in tds
        # do not call update again as it will go into an endless loop
        # function probably to take out later....
    
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
    def gw_subscription_changed(self, event, message_value):
        #pass
        logging.info('AnalyticsEngine:%s. val->[%s]' % (event, message_value))
        self.tds.update_datastore(message_value)
             
    def gw_subscriptions(self, event, message_value):
        logging.info('AnalyticsEngine:%s. Received event.' % (event))

        if self.initial_run:
            self.tds.update_datastore(message_value)
            self.initial_run = False

    #            
    # tws events     
    #
    def tickPrice(self, event, message_value):   
        #
        # dict: {'partition': 0, 'value': '{"field": 2, "price": 0.65, "canAutoExecute": 1, "tickerId": 1}', 'offset': 2151}
        #
        self.tds.set_symbol_price(json.loads(message_value['value']))
        #pass

 
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
      'clear_offsets':  True,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/ae.log'},
      'topics': ['tickPrice', 'gw_subscriptions', 'gw_subscription_changed', 'ae_req_tds_internal'],
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
    
          
        