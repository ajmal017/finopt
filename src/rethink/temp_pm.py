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
from numpy import average



class PortfolioItem():
    """
        Set up some constant variables
        
        position
        average cost
    
    """
    POSITION = 6001
    AVERAGE_COST = 6002
    POSITION_DELTA = 6003
    POSITION_THETA = 6004
    UNREAL_PL = 6005
    PERCENT_GAIN_LOSS = 6006
    AVERAGE_PRICE = 6007
    ACCOUNT_ID = 6008
    
        
    def __init__(self, account, contract_key, position, average_cost):
        
        self.contract_key = contract_key
        self.quantity = position
        self.average_cost = average_cost
        self.account_id = account
        
        contract = ContractHelper.makeContractfromRedisKeyEx(contract_key)
        if contract.m_secType == 'OPT':
            self.instrument = Option(contract)
        else: 
            self.instrument = Symbol(contract)
        
    
    def get_instrument(self):
        return self.instrument
        
    def get_instrument_type(self):
        return self.instrument.get_contract().m_secType
    
    def get_account(self):
        return self.account_id
        
    def calculate_pl(self):
        pass
    
    def set_position_cost(self, position, average_cost):
        self.quantity = position
        self.average_cost = average_cost   


class PortfolioMonitor(AbstractGatewayListener):

  
    '''
        portfolios : 
            portfolio: 
                account_id: 
                portfolio_item:
                
    '''
    
    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
        self.portfolios = {}
        self.option_chains = {}
        
    
    def test_oc(self, oc2):
        expiry = '20170427'
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
        
    
        
    
    
    def start_engine(self):
        self.twsc.start_manager()
        oc2 = OptionsChain('oc2')
        oc2.register_listener(self)
        self.test_oc(oc2)
        self.option_chains[oc2.name] = oc2
        
        try:
            logging.info('PortfolioMonitor:main_loop ***** accepting console input...')
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
                elif selection == '9': 
                    self.twsc.gw_message_handler.set_stop()
                    break
                else: 
                    pass                
                
                sleep(0.15)
            
        except (KeyboardInterrupt, SystemExit):
            logging.error('PortfolioMonitor: caught user interrupt. Shutting down...')
            self.twsc.gw_message_handler.set_stop() 
            logging.info('PortfolioMonitor: Service shut down complete...')               
    
    def is_position_in_portfolio(self, account, contract_key, position):
        try:
            return self.portfolios[account][contract_key]
        except KeyError:
            return None
            
    def get_portfolio(self, account):
        try:
            return self.portfolios[account]
        except KeyError:
            self.portfolios[account] = {}
        return self.portfolios[account]
    
    def deduce_option_underlying(self, option, map_type='symbol'):
        opt_underlying_map = {'symbol': {'HSI' : 'FUT', 'MHI' : 'FUT', 'QQQ' : 'STK'}}
        try:
            underlying_sectype = opt_underlying_map[map_type][option.get_contract().m_symbol]
            
        except KeyError:
            pass
        
                               
    
    def process_position(self, account, contract_key, position, average_cost):
        port = self.get_portfolio(account)
        if port:
            port_item =  self.is_position_in_portfolio(account, contract_key, position)
            if port_item:
                #
                port_item.set_position(position, average_cost)
                port_item.calculate_pl()
            else:
                port_item = PortfolioItem(account, contract_key, position, average_cost)
                instrument = port_item.get_instrument()
                self.tds.add_symbol(instrument)
                self.twsc.reqMktData(instrument, True)
                if port_item.get_instrument_type() == 'OPT':
                    '''
                        deduce option's underlying
                        resolve associated option chain by month, underlying
                        
                    '''
                    pass
                else:
                    port[contract_key] = port_item
                    
            
            
            
    
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
                logging.info('PortfolioMonitor:tds_event_tick_updated chain_id %s' % chain_id)
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
 
    def position(self, event, account, contract_key, position, average_cost, end_batch):
        self.process_position(account, contract_key, position, average_cost)
   
    def positionEnd(self, event): #, message_value):
        """ generated source for method positionEnd """
        logging.info('%s [[ %s ]]' % (event, vars()))

 
 
    def error(self, event, message_value):
        logging.info('PortfolioMonitor:%s. val->[%s]' % (event, message_value))         
        
        
if __name__ == '__main__':
    

    
    kwargs = {
      'name': 'portfolio_monitor',
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
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/pm.log'},
      'topics': ['tickPrice', 'tickSize'],
      'seek_to_end': ['*']

      
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
    
    
    server = PortfolioMonitor(kwargs)
    server.start_engine()
    
          
        