import logging
import json
from comms.ibc.base_client_messaging import AbstractGatewayListener





class AnalyticsEngine(AbstractGatewayListener):
    
    
    def __init__(self, name, kwargs):
        pass
    
    
    def ae_req_greeks(self):
        pass
    
    
    def gw_subscription_changed(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
 
 
            
    def gw_subscriptions(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
        items = json.loads(message_value)
        
        
    def tickPrice(self, event, message_value):   
        
        #'value': '{"tickerId": 0, "size": 3, "field": 3}'
        items = json.loads(message_value)
        tid = items['tickerId']

        try:
            contract_key = self.tickers[tid]
            #print contract_key
            map(lambda e: e.set_tick_value(items['field'], items['price']), self.symbols[contract_key]['syms'])
        except KeyError:
            pass

 
    def error(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))              