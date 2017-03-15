import logging
import json
import copy
from optparse import OptionParser
from time import sleep
from misc2.observer import Subscriber
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
        
    
    def start_engine(self):
        self.twsc.start_manager()
        self.request_subscrptions()

        try:
            logging.info('AnalyticsEngine:main_loop ***** accepting console input...')
            while True: 
            
                sleep(.45)
            
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
    def tds_event_new_symbol_added(self, event, items):
        pass
    
    def tds_event_tick_updated(self, event, items):
        pass
    
    #
    # external ae requests
    #
    def ae_req_greeks(self, event, message_value):
        #(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) 
        pass
    
    
    #
    # gateway events
    #
    def gw_subscription_changed(self, event, message_value):
        logging.info('AnalyticsEngine:%s. val->[%s]' % (event, message_value))
 
             
    def gw_subscriptions(self, event, message_value):
        logging.info('AnalyticsEngine:%s. val->[%s]' % (event, message_value))

        if self.initial_run:
            self.tds.update_datastore(message_value)
            self.initial_run = False

    #            
    # tws events     
    #
    def tickPrice(self, event, message_value):   
        self.tds.update_symbol_price(event, message_value)

 
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
      'logconfig': {'level': logging.INFO},
      'topics': ['gw_subscriptions', 'gw_subscription_changed'],
      'seek_to_end':['tickSize', 'tickPrice']
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
            
    #print kwargs    
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    server = AnalyticsEngine(kwargs)
    server.start_engine()
    
          
        