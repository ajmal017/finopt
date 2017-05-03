# -*- coding: utf-8 -*-
import sys, traceback
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
from rethink.portfolio_item import PortfolioItem, PortfolioRules
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener
from numpy import average


    


class PortfolioMonitor(AbstractGatewayListener):

  
    '''
        portfolios : 
             {
                <account_id>: {'port_items': {<contract_key>, PortItem}, 'opt_chains': {<oc_id>: option_chain}, 
                                'g_table':{'rows':{...} , 'cols':{...}, 'ckey_to_row_index':{'row_id':<row_id>, 'dirty': <true/false>, 'count':0}
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
        
        
    
    def start_engine(self):
        self.twsc.start_manager()
        self.twsc.reqPositions()
        self.starting_engine = True
        try:
            logging.info('PortfolioMonitor:main_loop ***** accepting console input...')
            menu = {}
            menu['1']="Request position" 
            menu['2']="Portfolio dump dtj"
            menu['3']="TDS dump"
            menu['4']="Request account updates"
            menu['5']="Table chart JSON"
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
                        #print self.dump_portfolio(acct)
                        print self.portfolios[acct]['g_table']
                elif selection == '3': 
                    print self.tds.dump()
                elif selection == '4': 
                    for acct in self.portfolios.keys():
                        self.twsc.reqAccountUpdates(True, acct)
                elif selection == '5':
                    for acct in self.portfolios.keys():
                        print self.g_datatable_json(acct)
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
        return self.get_portfolio_port_item(account, contract_key)
            
    def get_portfolio_port_item(self, account, contract_key):
        try:
            return self.portfolios[account]['port_items'][contract_key]
        except KeyError:
            return None
    
    def set_portfolio_port_item(self, account, contract_key, port_item):
        self.portfolios[account]['port_items'][contract_key] = port_item
        
        
    def create_empty_portfolio(self, account):
        port = self.portfolios[account] = {}
        self.portfolios[account]['port_items']=  {}
        self.portfolios[account]['opt_chains']=  {}
        self.portfolios[account]['g_table']=  {'ckey_to_row_index': {'count':0}}
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
                                    month,
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['trade_vol'])
            
            self.portfolios[account]['opt_chains'][oc_id] = oc 
            
            
        return oc
    
    def mark_gtable_row_dirty(self, account, contract_key, dirty=True):
        self.portfolios[account]['g_table'][contract_key]['dirty'] = dirty
        self.portfolios[account]['g_table'][contract_key]['count'] += 1
        return self.portfolios[account]['g_table'][contract_key]['row_id']
    
    def process_position(self, account, contract_key, position, average_cost, extra_info=None):
        
        # obtain a reference to the portfolio, if not exist create a new one 
        port = self.get_portfolio(account)
        port_item =  self.is_contract_in_portfolio(account, contract_key)

            
            
        if port_item:
            # update the values and recalculate p/l
            port_item.update_position(position, average_cost, extra_info)
            port_item.calculate_pl(contract_key)
            
            # update the affected row in gtable as changed 
            self.mark_gtable_row_dirty(True)
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
            return '[%s]: %s %s' % (x[0],  ', '.join('%s: %s' % (k,str(v)) for k, v in x[1].get_port_fields().iteritems()),
                                           ', '.join('%s: %s' % (k,str(v)) for k, v in x[1].get_instrument().get_tick_values().iteritems()))
        
        p_items = map(print_port_items, [x for x in self.portfolios[account]['port_items'].iteritems()])
        logging.info('PortfolioMonitor:dump_portfolio %s' % ('\n'.join(p_items)))
        return '\n'.join(p_items)
        
         
    
    def g_datatable_json(self, account):
    
        
         
        
        dtj = {'cols':[], 'rows':[], 'ckey_to_row_index':{}}
        header = [('symbol', 'Symbol', 'string'), ('right', 'Right', 'string'), ('avgcost', 'Avg Cost', 'number'), ('market_value', 'Market Value', 'number'), 
                  ('avgpx', 'Avg Price', 'number'), ('spotpx', 'Spot Price', 'number'), ('pos', 'Quantity', 'number'), 
                  ('delta', 'Delta', 'number'), ('theta', 'Theta', 'number'), ('gamma', 'Gamma', 'number'), 
                  ('pos_delta', 'P. Delta', 'number'), ('pos_theta', 'P. Theta', 'number'), ('pos_gamma', 'P. Gamma', 'number'), 
                  ('unreal_pl', 'Unreal P/L', 'number'), ('percent_gain_loss', '% gain/loss', 'number')  
                  ]  
        # header fields      
        map(lambda hf: dtj['cols'].append({'id': hf[0], 'label': hf[1], 'type': hf[2]}), header)
        
        
        def get_spot_px(x):
            px = float('nan')
            if x.get_quantity() > 0:
                px= x.get_instrument().get_tick_value(Symbol.BID)
            elif x.get_quantity() < 0:
                px= x.get_instrument().get_tick_value(Symbol.ASK)
            if px == -1:
                return x.get_instrument().get_tick_value(Symbol.LAST)
        
        # table rows
        def row_fields(x):
            
            rf = [{'v': '%s-%s-%s' % (x[1].get_symbol_id(), x[1].get_expiry(), x[1].get_strike())}, 
                 {'v': x[1].get_right()},
                 {'v': x[1].get_port_field(PortfolioItem.AVERAGE_COST)},
                 {'v': x[1].get_port_field(PortfolioItem.MARKET_VALUE)},
                 {'v': x[1].get_port_field(PortfolioItem.AVERAGE_PRICE)},
                 {'v': get_spot_px(x[1])},
                 {'v': x[1].get_quantity()},
                 {'v': x[1].get_instrument().get_tick_value(Option.DELTA)},
                 {'v': x[1].get_instrument().get_tick_value(Option.THETA)},
                 {'v': x[1].get_instrument().get_tick_value(Option.GAMMA)},
                 {'v': x[1].get_port_field(PortfolioItem.POSITION_DELTA)},
                 {'v': x[1].get_port_field(PortfolioItem.POSITION_THETA)},
                 {'v': x[1].get_port_field(PortfolioItem.POSITION_GAMMA)},
                 {'v': x[1].get_port_field(PortfolioItem.UNREAL_PL)},
                 {'v': x[1].get_port_field(PortfolioItem.PERCENT_GAIN_LOSS)}]
                 
             
            return rf 
        
        def set_contract_key_to_row_index(i):
            dtj['ckey_to_row_index'][p2_items[i].get_instrument().get_contract_key()]['row_id'] = i
            dtj['ckey_to_row_index'][p2_items[i].get_instrument().get_contract_key()]['dirty'] = False
        
        p_items = sorted([x for x in self.portfolios[account]['port_items'].iteritems()])
        p1_items = filter(lambda x: x[1].get_symbol_id() in self.kwargs['interested_position_types']['symbol'], p_items)
        p2_items = filter(lambda x: x[1].get_instrument_type() in self.kwargs['interested_position_types']['instrument_type'], p1_items)
        map(lambda p: dtj['rows'].append({'c': row_fields(p)}), p2_items)
        map(set_contract_key_to_row_index, range(len(p2_items)))
        
        return json.dumps(dtj) #, indent=4)            
    
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
                #logging.info('PortfolioMonitor:tds_event_tick_updated chain_id %s' % chain_id)
                
                for acct in self.portfolios:
                    
                    if chain_id  in self.portfolios[acct]['opt_chains'].keys():
                        #logging.info('PortfolioMonitor:tds_event_tick_updated --> portfolio opt_chains: [  %s  ] ' % 
                        #             str(self.portfolios[acct]['opt_chains'].keys()))
                        if 'FUT' in contract_key or 'STK' in contract_key:
                            results = self.portfolios[acct]['opt_chains'][chain_id].cal_greeks_in_chain(self.kwargs['evaluation_date'])
                        else:
                            results[ContractHelper.makeRedisKeyEx(s.get_contract())] =  self.portfolios[acct]['opt_chains'][chain_id].cal_option_greeks(s, self.kwargs['evaluation_date'])
                    #logging.info('PortfolioMonitor:tds_event_tick_updated. compute greek results %s' % results)
                        
                        #underlying_px = self.portfolios[acct]['opt_chains'][chain_id].get_underlying().get_tick_value(4)
                        
                    def update_portfolio_fields(key_greeks):
                        
                        self.tds.set_symbol_analytics(key_greeks[0], Option.IMPL_VOL, key_greeks[1][Option.IMPL_VOL])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.DELTA, key_greeks[1][Option.DELTA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.GAMMA, key_greeks[1][Option.GAMMA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.THETA, key_greeks[1][Option.THETA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.VEGA, key_greeks[1][Option.VEGA])
                        
                        if contract_key in self.portfolios[acct]['port_items']:
                            self.portfolios[acct]['port_items'][contract_key].calculate_pl(key_greeks[0]) #, underlying_px)
                        
                            # dispatch pm_event to listeners
                            self.mark_gtable_row_dirty(acct, contract_key, True)
                            logging.info('PortfolioMonitor:tds_event_tick_updated...marking the affected row %d:[%s] as dirty' %
                                                (self.portfolios[acct]['g_table']['dtj']['ckey_to_row_index']['row_id'], contract_key))
                            
                        
                    if results:
                        #logging.info('PortfolioMonitor:tds_event_tick_updated ....before map')
                        map(update_portfolio_fields, list(results.iteritems()))
                        #logging.info('PortfolioMonitor:tds_event_tick_updated ....after map')
                           
                               
                    
            else:
                logging.info('PortfolioMonitor:tds_event_tick_updated ignoring uninterested ticks %s' % contract_key)
                continue
             
        


    def tds_event_symbol_deleted(self, event, update_mode, name, instrument):
        pass

    #
    # gateway events
    #

    def tickPrice(self, event, contract_key, field, price, canAutoExecute):
        self.tds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)


    def tickSize(self, event, contract_key, field, size):
        #self.tds.set_symbol_tick_size(contract_key, field, size)
        #logging.info('MessageListener:%s. %s: %d %8.2f' % (event, contract_key, field, size))
        pass
 
    def position(self, event, account, contract_key, position, average_cost, end_batch):
        if not end_batch:
            #logging.info('PortfolioMonitor:position. received position message contract=%s' % contract_key)
            self.process_position(account, contract_key, position, average_cost)
   
        else:
            # to be run once during start up
            # subscribe to automatic account updates
            if self.starting_engine:
                for acct in self.portfolios.keys():
                    self.portfolios[acct]['g_table'] = self.g_datatable_json(acct)
                    logging.info('PortfolioMonitor:position. generate gtable for ac: [%s]' % acct)
                    self.twsc.reqAccountUpdates(True, acct)
                    logging.info('PortfolioMonitor:position. subscribing to auto updates for ac: [%s]' % acct)
            self.starting_engine = False
                    
    '''
        the 4 account functions below are invoked by AbstractListener.update_portfolio_account.
        the original message from TWS is first wrapped into update_portfolio_account event in 
        class TWS_event_handler and then expanded by AbstractListener.update_portfolio_account
        (check tws_event_hander)
    '''
    def raw_dump(self, event, items):
        del(items['self'])
        logging.info('%s [[ %s ]]' % (event, items))      
        
                
    def updateAccountValue(self, event, key, value, currency, account):  # key, value, currency, accountName):
        self.raw_dump(event, vars())
 
    def updatePortfolio(self, event, contract_key, position, market_price, market_value, average_cost, unrealized_PNL, realized_PNL, account):
        self.raw_dump(event, vars())
        self.process_position(account, contract_key, position, average_cost, 
                              {'market_price':market_price, 'market_value':market_value, 'unrealized_PNL': unrealized_PNL, 'realized_PNL': realized_PNL})
        
            
    def updateAccountTime(self, event, timestamp):
        self.raw_dump(event, vars())
        
    def accountDownloadEnd(self, event, account):  # accountName):
        self.raw_dump(event, vars())

 
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
      'topics': ['position', 'positionEnd', 'tickPrice', 'update_portfolio_account'],
      'seek_to_end': ['*'],
      'interested_position_types': {'symbol': ['HSI', 'MHI'], 'instrument_type': ['OPT', 'FUT']}

      
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
    
          
        