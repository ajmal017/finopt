import logging
import json
from time import sleep
from rethink.tick_datastore import TickDataStore
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener





class AnalyticsEngine(AbstractGatewayListener):

    AE_OPTIONS_CONFIG = {
        'underlying_substitution': {'IND': 'FUT'},
        'underlying_sub_list': ['HSI', 'MHI']
    }
    
    
    
    def __init__(self, name, kwargs):
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, name)
    
        self.tds = TickDataStore(name, self.twsc)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        self.twsc.add_listener_topics(self.tds, kwargs['tds_topics'])
 
        self.option_chains = {}
        
    
    def start_engine(self):
        self.twsc.start_manager()
        self.twsc.gw_req_subscriptions()
        self.initial_run = True
        while self.initial_run:
            sleep(0.5)


        try:
            logging.info('AnalyticsEngine:main_loop ***** accepting console input...')
            while True: 
            
                sleep(.45)
            
        except (KeyboardInterrupt, SystemExit):
            logging.error('AnalyticsEngine: caught user interrupt. Shutting down...')
            self.twsc.gw_message_handler.set_stop()
            
            logging.info('AnalyticsEngine: Service shut down complete...')               
    
    
    
    def ae_req_greeks(self, event, message_value):
        #(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) 
        pass
    
    def gw_subscription_changed(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
 
 
            
    def gw_subscriptions(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))

        if self.initial_run:
            self.tds.update_datastore(message_value)
            self.pending_gw_reply = False
            
        
    def tickPrice(self, event, message_value):   
        self.tds.update_symbol_price(event, message_value)

 
    def error(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))              