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

class PortfolioRules():
    rule_map = {
                'symbol': {'HSI' : 'FUT', 'MHI' : 'FUT', 'QQQ' : 'STK'},
                'expiry': {'HSI' : 'same_month', 'MHI': 'same_month', 'STK': 'leave_blank'},
                'option_structure': {
                                        'HSI': {'spd_size': 200, 'multiplier': 50, 'rate': 0.0012, 'div': 0},
                                        'MHI': {'spd_size': 200, 'multiplier': 10, 'rate': 0.0012, 'div': 0}
                                        
                                    },
                'exchange': {'HSI': 'HKFE', 'MHI': 'HKFE'}
                
               } 
    
    
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
        logging.info('PortfolioMonitor:calculate_pl. qty=%d avgcost=%8.4f' % (self.quantity, self.average_cost))
    
    def update_position(self, position, average_cost):
        self.quantity = position
        self.average_cost = average_cost   
        
    def dump(self):
        return 'PortfolioItem contents: %s %s %s' % (self.contract_key, self.quantity, self.average_cost)


class PortfolioMonitor(AbstractGatewayListener):

  
    '''
        portfolios : 
             {
                <account_id>: {'port_items': {<contract_key>, PortItem}, 'opt_chains': {<oc_id>: option_chain}}
             }   
                
    '''
   

    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        AbstractGatewayListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
        self.portfolios = {}
        
        
    
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
#         oc2 = OptionsChain('oc2')
#         oc2.register_listener(self)
#         self.test_oc(oc2)
#         self.option_chains[oc2.name] = oc2
        
        try:
            logging.info('PortfolioMonitor:main_loop ***** accepting console input...')
            menu = {}
            menu['1']="Request position" 
            menu['2']="Portfolio dump"
            menu['3']="TDS dump"
            menu['4']=""
            menu['9']="Exit"
            while True: 
                choices=menu.keys()
                choices.sort()
                for entry in choices: 
                    print entry, menu[entry]            

                selection = raw_input("Enter command:")
                if selection =='1':
                    self.twsc.reqPositions()
                elif selection == '2': 
                    for acct in self.portfolios.keys():
                        print self.dump_portfolio(acct)
                elif selection == '3': 
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
    
    def is_contract_in_portfolio(self, account, contract_key):
        return self.get_portfolio_port_items(account, contract_key)
            
    def get_portfolio_port_items(self, account, contract_key):
        try:
            return self.portfolios[account]['port_items'][contract_key]
        except KeyError:
            return None
    
    def set_portfolio_port_items(self, account, contract_key, port_item):
        self.portfolios[account]['port_items'][contract_key] = port_item
        
        
    def create_empty_portfolio(self, account):
        port = self.portfolios[account] = {}
        self.portfolios[account]['port_items']=  {}
        self.portfolios[account]['opt_chains']=  {}
        return port
                
    def get_portfolio(self, account):
        try:
            return self.portfolios[account]
        except KeyError:
            self.portfolios[account] = self.create_empty_portfolio(account)
        return self.portfolios[account]
    
    def deduce_option_underlying(self, option):
        '''
            given an Option object, return the underlying Symbol object
        '''
        

        try:
            symbol_id = option.get_contract().m_symbol
            underlying_sectype = PortfolioRules.rule_map['symbol'][symbol_id]
            exchange = option.get_contract().m_exchange
            currency = option.get_contract().m_currency
            expiry = option.get_contract().m_expiry if PortfolioRules.rule_map['expiry'][symbol_id] ==  'same_month' else ''
            contractTuple = (symbol_id, underlying_sectype, exchange, currency, expiry, 0, '')
            logging.info('PortfolioMonitor:deduce_option_underlying. Deduced underlying==> %s' %
                          str(contractTuple))
            return Symbol(ContractHelper.makeContract(contractTuple))
        except KeyError:
            logging.error('PortfolioMonitor:deduce_option_underlying. Unable to deduce the underlying for the given option %s' %
                          ContractHelper.printContract(option.get_contract))
            return None
        
        
    def is_oc_in_portfolio(self, account, oc_id):
        try:
            return self.portfolios[account]['opt_chains'][oc_id]
        except KeyError:
            return None
        
        
    def get_portfolio_option_chain(self, account, underlying):
        
        
        def create_oc_id(account, underlying_id, month):
            return '%s-%s-%s' % (account, underlying_id, month)
        
        underlying_id = underlying.get_contract().m_symbol
        month = underlying.get_contract().m_expiry
        oc_id = create_oc_id(account, underlying_id, month)
        oc = self.is_oc_in_portfolio(account, oc_id)
        if oc == None:
            oc = OptionsChain(oc_id)
            oc.register_listener(self)
            oc.set_option_structure(underlying.get_contract(),
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['spd_size'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['multiplier'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['rate'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['div'],
                                    month)
            
            self.portfolios[account]['opt_chains'][oc_id] = oc 
            
            
        return oc
    
    
    
    def process_position(self, account, contract_key, position, average_cost):
        
        # obtain a reference to the portfolio, if not exist create a new one 
        port = self.get_portfolio(account)
        port_item =  self.is_contract_in_portfolio(account, contract_key)

            
            
        if port_item:
            # update the values and recalculate p/l
            port_item.update_position(position, average_cost)
            port_item.calculate_pl()
        # new position 
        else:
            port_item = PortfolioItem(account, contract_key, position, average_cost)
            port['port_items'][contract_key] = port_item
            instrument = port_item.get_instrument()
            self.tds.add_symbol(instrument)
            self.twsc.reqMktData(instrument.get_contract(), True)
            # option position
            if port_item.get_instrument_type() == 'OPT':
                '''
                    deduce option's underlying
                    resolve associated option chain by month, underlying
                    
                '''
                underlying = self.deduce_option_underlying(instrument)
                if underlying:
                    oc = self.get_portfolio_option_chain(account, underlying)
                    
                    instrument.set_extra_attributes(OptionsChain.CHAIN_IDENTIFIER, oc.get_name())
                    oc.add_option(instrument)
                    
                    
                else:
                    logging.error('PortfolioMonitor:process_position. **** Error in adding the new position %s' % contract_key)
            # non options. stocks, futures that is...    
            else:
                logging.info('PortfolioMonitor:process_position. Adding a new non-option position into the portfolio [%s]' % port_item.dump())
                port['port_items'][contract_key] = port_item
                
            self.dump_portfolio(account)    
        
    def dump_portfolio(self, account):
        #<account_id>: {'port_items': {<contract_key>, instrument}, 'opt_chains': {<oc_id>: option_chain}}
        
        def print_port_items(x):
            return '[%s]: %4d %8.2f %s' % (x[0], x[1].quantity, 
                                           x[1].average_cost, 
                                           ' '.join('%s:%s' % (k,v) for k, v in x[1].get_instrument().get_tick_values().iteritems()))
        
        p_items = map(print_port_items, [x for x in self.portfolios[account]['port_items'].iteritems()])
        logging.info('PortfolioMonitor:dump_portfolio %s' % ('\n'.join(p_items)))
        return '\n'.join(p_items)
        
         
    
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
                
                for acct in self.portfolios:
                    
                    if chain_id  in self.portfolios[acct]['opt_chains'].keys():
                        logging.info('PortfolioMonitor:tds_event_tick_updated --> portfolio opt_chains: [  %s  ] ' % 
                                     str(self.portfolios[acct]['opt_chains'].keys()))
                        if 'FUT' in contract_key or 'STK' in contract_key:
                            results = self.portfolios[acct]['opt_chains'][chain_id].cal_greeks_in_chain(self.kwargs['evaluation_date'])
                        else:
                            results[ContractHelper.makeRedisKeyEx(s.get_contract())] =  self.portfolios[acct]['opt_chains'][chain_id].cal_option_greeks(s, self.kwargs['evaluation_date'])
                    logging.info('PortfolioMonitor:tds_event_tick_updated. compute greek results %s' % results)    
                    # set_analytics(self, imvol=None, delta=None, gamma=None, theta=None, vega=None, npv=None):
                    # 
                    def update_tds_analytics(key_greeks):
                        
                        self.tds.set_symbol_analytics(key_greeks[0], Option.IMPL_VOL, key_greeks[1][Option.IMPL_VOL])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.DELTA, key_greeks[1][Option.DELTA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.GAMMA, key_greeks[1][Option.GAMMA])
                        self.tds.set_2symbol_analytics(key_greeks[0], Option.THETA, key_greeks[1][Option.THETA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.VEGA, key_greeks[1][Option.VEGA])
                        
                    map(update_tds_analytics, list(results.iteritems()))                

            else:
                
                continue
             
        


    def tds_event_symbol_deleted(self, event, update_mode, name, instrument):
        pass

    #
    # gateway events
    #

    def tickPrice(self, event, contract_key, field, price, canAutoExecute):
        logging.info('MessageListener:%s. %s %d %8.2f' % (event, contract_key, field, price))
        self.tds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)


    def tickSize(self, event, contract_key, field, size):
        self.tds.set_symbol_tick_size(contract_key, field, size)
        #logging.info('MessageListener:%s. %s: %d %8.2f' % (event, contract_key, field, size))
 
    def position(self, event, account, contract_key, position, average_cost, end_batch):
        if not end_batch:
            logging.info('PortfolioMonitor:position. received position message contract=%s' % contract_key)
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
      'group_id': 'PM',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/pm.log'},
      'topics': ['position', 'positionEnd', 'tickPrice'],
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
    
          
        