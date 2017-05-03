import logging
import json
import time, datetime
import copy
from optparse import OptionParser
from time import sleep
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from finopt.instrument import Symbol, Option
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener





class AnalyticsEngine(AbstractGatewayListener):

  
    
    
    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
        
        self.option_chains = {}
        
    
    def test_oc(self, oc2):
        expiry = '20170529'
        contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', expiry, 0, '')
        contract = ContractHelper.makeContract(contractTuple)  
        
        oc2.set_option_structure(contract, 200, 50, 0.0012, 0.0328, expiry)        
        
        oc2.build_chain(24172, 0.04, 0.22)
        
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
        
    
    def test_oc3(self, oc3):
        expiry = '20170629'
        contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', expiry, 0, '')
        contract = ContractHelper.makeContract(contractTuple)  
         
        oc3.set_option_structure(contract, 200, 50, 0.0012, 0.0328, expiry)        
         
        oc3.build_chain(24172, 0.04, 0.22)

#         expiry = '20170331'
#         contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
# 
# 
#         contract = ContractHelper.makeContract(contractTuple)  
#         
#         oc3.set_option_structure(contract, 0.5, 100, 0.0012, 0.0328, expiry)        
#         
#         oc3.build_chain(130, 0.03, 0.22)
        
#         expiry='20170324'
#         contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
#         contract = ContractHelper.makeContract(contractTuple)  
# 
#         oc2.set_option_structure(contract, 0.5, 100, 0.0012, 0.0328, expiry)        
#     
#         oc2.build_chain(132.11, 0.02, 0.22)
        
        
        oc3.pretty_print()        

        for o in oc3.get_option_chain():
            self.tds.add_symbol(o)
        self.tds.add_symbol(oc3.get_underlying())
        
    
    
    def start_engine(self):
        self.twsc.start_manager()
        oc2 = OptionsChain('oc2')
        oc2.register_listener(self)
        self.test_oc(oc2)
        oc3 = OptionsChain('oc3')
        oc3.register_listener(self)
        self.test_oc3(oc3)
        self.option_chains[oc2.name] = oc2
        self.option_chains[oc3.name] = oc3
        
        try:
            logging.info('AnalyticsEngine:main_loop ***** accepting console input...')
            menu = {}
            menu['1']="Display option chain oc2" 
            menu['2']="Display tick data store "
            menu['3']="Display option chain oc3"
            menu['4']="Generate oc3 gtable json"
            menu['9']="Exit"
            while True: 
                choices=menu.keys()
                choices.sort()
                for entry in choices: 
                    print entry, menu[entry]            

                selection = raw_input("Enter command:")
                if selection =='1':
                    oc2.pretty_print()
                elif selection == '2': 
                    self.tds.dump()
                elif selection == '3':
                    oc3.pretty_print()
                elif selection == '4':
                    print oc3.g_datatable_json()
                elif selection == '9': 
                    self.twsc.gw_message_handler.set_stop()
                    break
                else: 
                    oc3.pretty_print()                
                
                sleep(0.15)
            
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
        
    
    def tds_event_tick_updated(self, event, contract_key, field, price, syms):
        
        for s in syms:
            
            if OptionsChain.CHAIN_IDENTIFIER in s.get_extra_attributes():
                results = {}
                chain_id = s.get_extra_attributes()[OptionsChain.CHAIN_IDENTIFIER]
                logging.info('AnalyticsEngine:tds_event_tick_updated chain_id %s' % chain_id)
                if chain_id  in self.option_chains.keys():
                    if 'FUT' in contract_key or 'STK' in contract_key:
                        results = self.option_chains[chain_id].cal_greeks_in_chain(self.kwargs['evaluation_date'])
                    else:
                        results[ContractHelper.makeRedisKeyEx(s.get_contract())] = self.option_chains[chain_id].cal_option_greeks(s, self.kwargs['evaluation_date'])
                logging.info('AnalysticsEngine:tds_event_tick_updated. compute greek results %s' % results)    
                # set_analytics(self, imvol=None, delta=None, gamma=None, theta=None, vega=None, npv=None):
                # 
                def update_tds_analytics(key_greeks):
                    
                    self.tds.set_symbol_analytics(key_greeks[0], Option.IMPL_VOL, key_greeks[1][Option.IMPL_VOL])
                    self.tds.set_symbol_analytics(key_greeks[0], Option.DELTA, key_greeks[1][Option.DELTA])
                    self.tds.set_symbol_analytics(key_greeks[0], Option.GAMMA, key_greeks[1][Option.GAMMA])
                    self.tds.set_symbol_analytics(key_greeks[0], Option.THETA, key_greeks[1][Option.THETA])
                    self.tds.set_symbol_analytics(key_greeks[0], Option.VEGA, key_greeks[1][Option.VEGA])
                    
                map(update_tds_analytics, list(results.iteritems()))                

            else:
                
                continue
             
        


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
        logging.debug('MessageListener:%s. %s %d %8.2f' % (event, contract_key, field, price))
        self.tds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)


    def tickSize(self, event, contract_key, field, size):
        self.tds.set_symbol_tick_size(contract_key, field, size)
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
      'topics': ['tickPrice', 'tickSize'],
      'seek_to_end': ['*']

      #'seek_to_end':['tickSize', 'tickPrice','gw_subscriptions', 'gw_subscription_changed']
      }

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--clear_offsets", action="store_true", dest="clear_offsets",
                      help="delete all redis offsets used by this program")
    parser.add_option("-g", "--group_id",
                      action="store", dest="group_id", 
                      help="assign group_id to this running instance")
    parser.add_option("-e", "--evaluation_date",
                     action="store", dest="evaluation_date", 
                     help="specify evaluation date for option calculations")   
    
    (options, args) = parser.parse_args()
    if options.evaluation_date == None:
        options.evaluation_date = time.strftime('%Y%m%d') 
    
    for option, value in options.__dict__.iteritems():
        if value <> None:
            kwargs[option] = value
    
    
            
  
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    
    server = AnalyticsEngine(kwargs)
    server.start_engine()
    
          
        