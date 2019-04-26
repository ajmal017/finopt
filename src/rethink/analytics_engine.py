import logging
import json
import time, datetime
import copy
from optparse import OptionParser
from time import sleep
from datetime import datetime
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from finopt.instrument import Symbol, Option
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener
import sys, traceback
import redis
import uuid


class DataCapture():
    # requires redis as persistence
    def __init__(self, rs):
        self.rs = rs
        self.requesters = {}
        self.tickcount = 0

    def register(self, request_id, max_ticks = 30, interval = 5):
        self.requesters[request_id] = {'max_ticks': max_ticks, 'interval': interval, 'last_checktime':datetime.now(),
                                       'last_tickcount': 0}
        
    
    def update_tickcount(self):
        self.tickcount += 1    
        
    def record_parity(self, option_chains, request_id):
        
        if self.is_allow_record(request_id):
            for oc in option_chains:
                last_px = oc.get_underlying().get_tick_value(4)
                pc_errors = oc.cal_put_call_parity(last_px)
                self.rs.rpush('parity', (pc_errors , oc.get_expiry(), last_px, time.strftime("%Y%m%d%H%M%S")))
    
    def test_dummy(self, request_id):
        if self.is_allow_record(request_id):
            print 'valid %s ' % time.strftime("%Y%m%d%H%M%S")
        else:
            print 'invalid'
            
        
    def is_allow_record(self, request_id):
        rq_config = self.requesters[request_id]
        now = datetime.now()
        delta =  now - rq_config['last_checktime']
        if delta.total_seconds() > rq_config['interval'] or rq_config['max_ticks'] < (self.tickcount - rq_config['last_tickcount']):
            rq_config['last_checktime'] = now 
            rq_config['last_tickcount'] = self.tickcount
            return True
        return False



class AnalyticsEngine(AbstractGatewayListener):

  
    
    
    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
        
        self.option_chains = {}
        self.dc = DataCapture(redis.Redis(kwargs['redis_host'],
                                                kwargs['redis_port'],
                                                kwargs['redis_db']))
        
        self.parity_id = 'parity'
        self.dc.register(self.parity_id)

        
    
    def test_oc(self, oc2):
        expiry = '20190328'
        contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', expiry, 0, '')
        contract = ContractHelper.makeContract(contractTuple)  
        
        oc2.set_option_structure(contract, 200, 50, 0.0012, 0.0328, expiry)        
        
        oc2.build_chain(27000, 0.04, 0.22)
        
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
        expiry = '20190227'
        contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', expiry, 0, '')
        contract = ContractHelper.makeContract(contractTuple)  
         
        oc3.set_option_structure(contract, 200, 50, 0.0012, 0.0328, expiry)        
         
        oc3.build_chain(27000, 0.06, 0.22)

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
        
    def init_oc_from_params(self, **args):
        oc = OptionsChain(args['name'])
        contractTuple = (args['m_symbol'], args['m_secType'], args['m_exchange'], args['m_currency'], 
                         args['m_expiry'], args['m_strike'], args['m_right'])
        contract = ContractHelper.makeContract(contractTuple)  
        #underlying, spd_size, multiplier, rate, div, expiry, trade_vol=0.15)
        oc.set_option_structure(contract, args['spd_size'], args['multiplier'], args['rate'], args['div'], args['m_expiry'], 
                                args['trade_vol'])                                                  
        oc.build_chain(args['undly_price'], args['bound'], args['trade_vol'])
        
            
        for o in oc.get_option_chain():
            self.tds.add_symbol(o)
        self.tds.add_symbol(oc.get_underlying())
        return oc
    
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
            menu['5']="Display oc3 put call parity"
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
                elif selection == '5':
                    try:
                        last_px = oc3.get_underlying().get_tick_value(4)
                        results = oc3.cal_put_call_parity(last_px)
                        print results
                        print '\n'.join('%0.0f:%0.2f' % (k, v) for k, v in sorted(results.items()))
                    except:
                        logging.error(traceback.format_exc())
                    
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
        if field not in [Symbol.ASK, Symbol.BID, Symbol.LAST]:
            return

        # increment the tick counter by 1
        # for use in record_parity checking threshold value
        self.dc.update_tickcount()
                
        for s in syms:
            
            if OptionsChain.CHAIN_IDENTIFIER in s.get_extra_attributes():
                results = {}
                pc_errors = None
                chain_id = s.get_extra_attributes()[OptionsChain.CHAIN_IDENTIFIER]
                logging.info('AnalyticsEngine:tds_event_tick_updated chain_id %s' % chain_id)
                if chain_id  in self.option_chains.keys():
                    if 'FUT' in contract_key or 'STK' in contract_key:
                        

                        try:
                            self.dc.record_parity([self.option_chains[chain_id]], self.parity_id)
                        except:
                            pass
                        
                        results = self.option_chains[chain_id].cal_greeks_in_chain(self.kwargs['evaluation_date'], price)
                        
                        
                    else:
                        results[ContractHelper.makeRedisKeyEx(s.get_contract())] = self.option_chains[chain_id].cal_option_greeks\
                                                                                        (s, self.kwargs['evaluation_date'], float('nan'), price)
                        
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
      'bootstrap_host': 'vorsprung',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'vsu-bison',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'AE',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/ae.log'},
      'topics': ['tickPrice', 'tickSize'],
      'seek_to_end': ['*'],
#       'ocs':[
#             name, underlying, spd_size, multiplier, rate, div, expiry, trade_vol)
#             ]

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
    
          
        