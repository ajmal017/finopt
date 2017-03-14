# -*- coding: utf-8 -*-

import sys
import ConfigParser
import json
import logging
import threading
from ib.ext.Contract import Contract
from misc2.helpers import ContractHelper, dict2str
from finopt.instrument import Symbol, Option
from comms.ibc.base_client_messaging import AbstractGatewayListener
from misc2.observer import Publisher, Subscriber 
#from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper

from time import sleep
import time, datetime
import finopt.optcal
import traceback
import redis

               
        

class OptionsChain(Publisher):
    underlying = None
    spd_size = None
    multiplier = None
    
    #
    # options is a list containing Option object
    options = None
    
    id = None
    div = 0.0
    rate = 0.0
    expiry = None
    trade_vol = None
    #iv = optcal.cal_implvol(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, vol, premium)
    
    option_chain_events = ('on_option_added', 'on_option_deleted', 'on_option_updated')
    
    def __init__(self, id):
        self.id = id
        self.options = []
        Publisher.__init__(self, OptionsChain.option_chain_events)
        

    
    def get_id(self):
        return self.id
    
    def get_underlying(self):
        return self.underlying
    
    def set_underlying(self, contract):
        #self.underlying = contract
        self.underlying = Symbol(contract)

        
        
    def set_spread_table(self, spd_size, multiplier):
        self.spd_size = spd_size
        self.multiplier = multiplier
    
    def set_div(self, div):
        self.div = div
    
    def set_rate(self, rate):
        self.rate = rate
        
    def set_expiry(self, expiry):
        self.expiry = expiry
    
    def set_trade_vol(self, tvol):
        self.trade_vol = tvol
        
    def set_option_structure(self, underlying, spd_size, multiplier, rate, div, expiry):
        self.set_div(div)
        self.set_rate(rate)
        self.set_spread_table(spd_size, multiplier)
        self.set_underlying(underlying)
        self.set_expiry(expiry)
      
    def build_chain(self, px, bound, trade_vol):
        self.set_trade_vol(trade_vol)
        undlypx = round(px  / self.spd_size) * self.spd_size
        upper_limit = undlypx * (1 + bound)
        lower_limit = undlypx * (1 - bound)          
        
        
        
        base_opt_contract = json.loads(ContractHelper.object2kvstring(self.get_underlying().get_contract()))
        
        #
        #     notify listener(s) the option's underlying
        #     allowing the listeners to store the reference to OptionsChain underlying 
        #
        self.dispatch(OptionsChain.option_chain_events[0], self.get_underlying())
        #
        #
        #
        
        
        #for i in self.xfrange(int(undlypx), int(upper_limit ), self.spd_size):
        for i in self.xfrange(undlypx, upper_limit, self.spd_size):

            base_opt_contract['m_secType'] = 'OPT'
            base_opt_contract['m_strike'] = i
            base_opt_contract['m_expiry'] = self.expiry
            base_opt_contract['m_right'] = 'C'
            base_opt_contract['m_multiplier'] = self.multiplier
            
            #self.options.append(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
            self.add_option(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
            
            base_opt_contract['m_right'] = 'P'
            #self.options.append(ContractHelper.kv2object(base_opt_contract, Contract))
            #self.options.append(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
            self.add_option(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
 
        
        for i in self.xfrange(undlypx - self.spd_size, lower_limit, -self.spd_size):      
            #print i, lower_limit
            base_opt_contract['m_secType'] = 'OPT'
            base_opt_contract['m_strike'] = i
            base_opt_contract['m_expiry'] = self.expiry
            base_opt_contract['m_right'] = 'C'
            base_opt_contract['m_multiplier'] = self.multiplier
            #self.options.append(ContractHelper.kv2object(base_opt_contract, Contract))
            #self.options.append(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
            self.add_option(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
             
            base_opt_contract['m_right'] = 'P'
            #self.options.append(ContractHelper.kv2object(base_opt_contract, Contract))
            #self.options.append(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
            self.add_option(Option(ContractHelper.kv2object(base_opt_contract, Contract)))
        
        

        

    def xfrange(self, start, stop=None, step=None):
        if stop is None:
            stop = float(start)
            start = 0.0
        
        if step is None:
            step = 1.0
        
        cur = float(start)
        if start <= stop:
            while cur < stop:
                yield cur
                cur += step
        else:
            while cur > stop:
                yield cur
                cur += step
            
    def get_option_chain(self):
        return self.options

        
    def add_option(self, option):
        #events = ('on_option_added', 'on_option_deleted', 'on_option_updated')
        #
        # 
        self.options.append(option)
        self.dispatch(OptionsChain.option_chain_events[0], option)
    
    
    def pretty_print(self):
        sorted_opt = sorted(map(lambda i: (self.options[i].get_contract().m_strike, self.options[i]) , range(len(self.options))))
        
        def format_tick_val(val, fmt):
            if val == None:
                length = len(fmt % (0))
                return ' ' * length
            
            return fmt % (val) 
        


        
        sorted_call = filter(lambda x: x[1].get_contract().m_right == 'C', sorted_opt)
        sorted_put = filter(lambda x: x[1].get_contract().m_right == 'P', sorted_opt)
        # last, bidq, bid, ask, askq, imvol, delta, theta
        fmt_spec = '%8.2f'
        fmt_spec2 = '%8.4f'
        fmt_specq = '%8d'
        fmt_call = map(lambda x: (x[0], '%s,%s,%s,%s,%s,%s,%s,%s' % (format_tick_val(x[1].get_tick_value(4), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(0), fmt_specq),
                                               format_tick_val(x[1].get_tick_value(1), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(2), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(3), fmt_specq),
                                               format_tick_val(x[1].get_analytics()[Option.IMPL_VOL], fmt_spec2),
                                               format_tick_val(x[1].get_analytics()[Option.DELTA], fmt_spec2),
                                               format_tick_val(x[1].get_analytics()[Option.THETA], fmt_spec2),
                                               )), sorted_call)
        
        fmt_put = map(lambda x: (x[0], '%s,%s,%s,%s,%s,%s,%s,%s' % (format_tick_val(x[1].get_tick_value(4), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(0), fmt_specq),                                                                                                                  
                                               format_tick_val(x[1].get_tick_value(1), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(2), fmt_spec),
                                               format_tick_val(x[1].get_tick_value(3), fmt_specq),
                                               format_tick_val(x[1].get_analytics()[Option.IMPL_VOL], fmt_spec2),
                                               format_tick_val(x[1].get_analytics()[Option.DELTA], fmt_spec2),
                                               format_tick_val(x[1].get_analytics()[Option.THETA], fmt_spec2),                    
                                               )), sorted_put)
        
        undlypx = '%s,%s,%s,%s,%s' % (format_tick_val(self.get_underlying().get_tick_value(4), fmt_spec), 
                                  format_tick_val(self.get_underlying().get_tick_value(0), fmt_specq),
                                           format_tick_val(self.get_underlying().get_tick_value(1), fmt_spec),
                                           format_tick_val(self.get_underlying().get_tick_value(2), fmt_spec),
                                           format_tick_val(self.get_underlying().get_tick_value(3), fmt_spec)
                                )
        
        #title = '%s%30s%s%s' % ('-' * 40, ContractHelper.makeRedisKeyEx(self.get_underlying().get_contract()).center(50, ' '), undlypx, '-' * 40) 
        title = '%s%30s%s%s' % ('-' * 41, ContractHelper.makeRedisKeyEx(self.get_underlying().get_contract()).center(42, ' '), undlypx, '-' * 27)
        header = '%8s|%8s|%8s|%8s|%8s|%8s|%8s|%8s |%8s| %8s|%8s|%8s|%8s|%8s|%8s|%8s|%8s' % ('last', 'bidq', 'bid', 'ask', 'askq', 'ivol', 'delta', 'theta', 'strike', 'last', 'bidq', 'bid', 'ask', 'askq', 'ivol', 'delta', 'theta')
        combined = map(lambda i: '%s |%8.2f| %s' % (fmt_call[i][1], fmt_put[i][0], fmt_put[i][1]), range(len(fmt_call)) )
        footer = '%s' % ('-' * 154) 
        print title
        print header
        for e in combined:
            print e
        print footer
        
        
    def generate_google_datatable_json(self):
        
        sorted_opt = sorted(map(lambda i: (self.options[i].get_contract().m_strike, self.options[i]) , range(len(self.options))))
        
        sorted_call = filter(lambda x: x[1].get_contract().m_right == 'C', sorted_opt)
        sorted_put = filter(lambda x: x[1].get_contract().m_right == 'P', sorted_opt)
        

        
        dtj = {'cols':[], 'rows':[]}
        header = [('last', 'number'), ('bidq', 'number'), ('bid', 'number'), 
                  ('ask', 'number'), ('askq', 'number'), ('ivol', 'number'), 
                  ('delta', 'number'), ('theta', 'number'), ('strike', 'number'), 
                  ('last', 'number'), ('bidq', 'number'), ('bid', 'number'), 
                  ('ask', 'number'), ('askq', 'number'), ('ivol', 'number'), 
                  ('delta', 'number'), ('theta', 'number')
                  ]  
        # header fields      
        map(lambda hf: dtj['cols'].append({'id': hf[0], 'label': hf[0], 'type': hf[1]}), header)
        
        
        # table rows
        # arrange each row with C on the left, strike in the middle, and P on the right
        def row_fields(x):
            
            rf = [{'v': x[1].get_tick_value(4)}, 
                 {'v': x[1].get_tick_value(0)},
                 {'v': x[1].get_tick_value(1)},
                 {'v': x[1].get_tick_value(2)},
                 {'v': x[1].get_tick_value(3)},
                 {'v': x[1].get_analytics()[Option.IMPL_VOL]},
                 {'v': x[1].get_analytics()[Option.DELTA]},
                 {'v': x[1].get_analytics()[Option.THETA]}]                 
                 
             
            return rf 
        
        map(lambda i: dtj['rows'].append({'c': row_fields(sorted_call[i]) +
                                                [{'v': sorted_call[i][0]}] + 
                                                row_fields(sorted_put[i])}), range(len(sorted_call)))
    
        
        print json.dumps(dtj) #, indent=4)
        


class OCConsumer(Subscriber, AbstractGatewayListener):
    symbols = {}
    tickers = {}
    
    """
    
    Data structure:
        tickers map contains key value pairs of ticker id mapped to Symbol primary key
        tickers => {id1: key1, id2:key2...}
        
        example: tickers = {9: 'QQQ-20170217-127.00-C-OPT-USD-SMART-102'
                            43: 'QQQ-20170217-124.00-C-OPT-USD-SMART-102' ...}
                            
        symbols map contains key value pairs of Symbol primary key mapped to a dict object.
        The dict object contains the ticker id and a list of Symbol objects associated with ticker_id
        symbols => {key1: 
                        { 'ticker_id': id1, 
                          'syms' : [<object ref to Symbol1>,<object ref to Symbol2>...]
                        }
                    key2:
                        ...
                   }
        
        example: symbols = {'QQQ-20170217-127.00-C-OPT-USD-SMART-102':
                                {'ticker_id': 9, 
                                 'syms': [<object ref to Symbol QQQ>, ...]
                                }
                            }
                            
        Usage:
        Given a ticker_id, the Symbol key can be looked up from tickers
        With the Symbol key obtained, the reference to the actual object associated with the ticker_id can be retrieved
        by looking up from symbols[key]['syms']
        
        speed: 2 x O(1) + n
    
    
    """

    def __init__(self, name):
        Subscriber.__init__(self, name)
        AbstractGatewayListener.__init__(self, name)
        
    

    def dump(self):
            #print ', '.join('[%s:%s]' % (k, v['ticker_id'])) 
        logging.debug('OCConsumer-symbols: [Key: Ticker ID: # options objects]: ---->\n%s' % (',\n'.join('[%s:%d:%d]' % (k, v['ticker_id'],len(v['syms'])) for k,v in self.symbols.iteritems())))
        logging.debug('OCConsumer-tickers: %s' % self.tickers)
        
    def on_option_chain_changed(self, message, symbol=None):
        key = symbol.get_key()
        #print key
        if key not in self.symbols:
            self.symbols[key]= {'ticker_id': -1, 'syms': []}
#         self.symbols[key]['ticker_id'] = -1
        self.symbols[key]['syms'].append(symbol)        
        logging.debug('OCConsumer: update event %s: %s %s' % (self.name,message, "none" if not symbol else symbol.get_key()))
    
        
        
    def tickPrice(self, event, message_value):   
        
        #'value': '{"tickerId": 0, "size": 3, "field": 3}'
        items = json.loads(message_value)
        tid = items['tickerId']
        #print tid
#         if tid in self.tickers:
#             contract_key = self.tickers[tid]
#             #print contract_key
#             for e in self.symbols[contract_key]['syms']:
#                 e.set_tick_value(items['field'], items['price'])
        try:
            contract_key = self.tickers[tid]
            #print contract_key
            map(lambda e: e.set_tick_value(items['field'], items['price']), self.symbols[contract_key]['syms'])
        except KeyError:
            pass
            

 
    def error(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))  



    def get_id_contracts(self, message_value):
        try:
            id_contracts = json.loads(message_value)
            
            def utf2asc(x):
                return x if isinstance(x, unicode) else x
            
            return map(lambda x: (x[0], ContractHelper.kvstring2contract(utf2asc(x[1]))), id_contracts)
        except TypeError:
            logging.error('SubscriptionManager:get_id_contracts. Exception when trying to get id_contracts from redis ***')
            return None


    
    
    
    
    def gw_subscription_changed(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
 
 
            
    def gw_subscriptions(self, event, message_value):
        logging.info('MessageListener:%s. val->[%s]' % (event, message_value))
        items = json.loads(message_value)

    

    
        
#    def gw_subscription_changed(self, items):
        # <class 'comms.tws_protocol_helper.Message'>
        # sample
        #{0: {'contract': <ib.ext.Contract.Contract object at 0x7ff8f8c9e210>}, 1: {'contract': <ib.ext.Contract.Contract object at 0x7ff8f8c9e250>},... }
        #print items.__dict__['subscriptions']
        """
        {0: {u'm_conId': 0, u'm_right': u'', u'm_symbol': u'QQQ', 
        u'm_secType': u'STK', u'm_includeExpired': False, 
        u'm_expiry': u'', u'm_currency': u'USD', u'm_exchange': u'SMART', u'm_strike': 0}, 
        1: {u'm_conId': 0, u'm_right': u'C', u'm_symbol': u'QQQ', u'm_secType': u'OPT', 
        u'm_includeExpired': False, u'm_multiplier': 100, u'm_expiry': u'20170217', u'm_currency': u'USD', u'm_exchange': u'SMART', u'm_strike': 125.0}, 
        2: {u'm_conId': 0, u'm_right': u'P', u'm_symbol': u'QQQ', u'm_secType': u'OPT', u'm_includeExpired': False, u'm_multiplier': 100, 
        u'm_expiry': u'20170217', u'm_currency': u'USD', u'm_exchange': u'SMART', u'm_strike': 125.0}, 
        ...
         
        78: {u'm_conId': 0, u'm_right': u'P', u'm_symbol': u'QQQ', u'm_secType': u'OPT', 
        u'm_includeExpired': False, u'm_multiplier': 100, u'm_expiry': u'20170217', 
        u'm_currency': u'USD', u'm_exchange': u'SMART', u'm_strike': 115.5}}
        tickPrice>> [0:QQQ--0.00--STK-USD-SMART-102] bid_q=-1.0000 [2017-01-28 12:08:49.587014]

        """


        for tid, con in items.iteritems():
            contract = ContractHelper.kv2contract(con)
            key = ContractHelper.makeRedisKeyEx(contract)
            if key in self.symbols: 
                self.symbols[key]['ticker_id'] = tid
                self.tickers[tid] = key
        
        
                
    def req_option_greeks(self, event, message_value):
        pass


        


         
              
              
              
def unit_test1():
    
    fn = open('../../data/mock_msg/mock_msg.txt')
    lines = map(lambda x: x.split('|'), filter(lambda x: x[0] <> '#', fn.readlines()))
    mock_msg_str = filter(lambda x: x[0] == 'gw_subscription_changed', lines)[0]
    #print mock_msg_str
    mock_msg = eval(mock_msg_str[1])

    
    dc = OCConsumer('dummy consumer')



    expiry = '20170217'
    contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
    contract = ContractHelper.makeContract(contractTuple)  
    oc2 = OptionsChain('qqq-%s' % expiry)
    oc2.set_option_structure(contract, 0.5, 100, 0.0012, 0.0328, expiry)


    for i in range(len(OptionsChain.option_chain_events)):
        oc2.register(OptionsChain.option_chain_events[i], dc, dc.on_option_chain_changed)
        

    
    oc2.build_chain(125, 0.02, 0.22)
#     for c in oc2.get_option_chain():
#         print '%s' % ContractHelper.makeRedisKeyEx(c.get_contract())



    for right in ['C','P']:    
        optionTuple = ('QQQ', 'OPT', 'SMART', 'USD', expiry, 130.5, right)
        o = Option(ContractHelper.makeContract(optionTuple))
        oc2.add_option(o)
#     optionTuple = ('HSI', 'OPT', 'HKFE', 'HKD', far_expiry, 23000, 'C')
#     o = Option(ContractHelper.makeContract(optionTuple))
#     oc2.add_option(o)
    
    
    dc.gw_subscription_changed(mock_msg)
     
    mock_items= {'field':4, 'typeName':'tickPrice', 'price':1.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':79, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    mock_items= {'field':4, 'typeName':'tickPrice', 'price':125.82, 'ts':1485661437.83, 'source':'IB', 'tickerId':0, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    mock_items= {'field':2, 'typeName':'tickPrice', 'price':125.72, 'ts':1485661437.83, 'source':'IB', 'tickerId':0, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    mock_items= {'field':1, 'typeName':'tickPrice', 'price':124.72, 'ts':1485661437.83, 'source':'IB', 'tickerId':0, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    
    mock_items= {'field':4, 'typeName':'tickPrice', 'price':1.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':3, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    mock_items= {'field':2, 'typeName':'tickPrice', 'price':1.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':5, 'canAutoExecute':0}
    dc.tickPrice(mock_items)
    mock_items= {'field':1, 'typeName':'tickPrice', 'price':1.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':10, 'canAutoExecute':0}
    dc.tickPrice(mock_items) 
    dc.dump()     
    oc2.pretty_print()      
     
    
if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0: 
        raise ValueError, "Failed to open config file" 
    
   
      
    logconfig = eval(config.get("options_chain", "options_calculation_engine.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'
    logconfig['level'] = logging.DEBUG
    logging.basicConfig(**logconfig)        
        
    
#     contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
#     
#     contract = ContractHelper.makeContract(contractTuple)  
#     oc = OptionsChain('QQQ-MAR24')
#     
#     oc.set_option_structure(contract, 0.5, 100, 0.005, 0.003, '20160324')
#     oc.build_chain(98.0, 0.025, 0.25)
#     
#     for c in oc.get_option_chain():
#         print '%s' % ContractHelper.makeRedisKeyEx(c.get_contract())
    
    unit_test1()
    

#     near_expiry = '20160226'
#     contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', near_expiry, 0, '')
#     contract = ContractHelper.makeContract(contractTuple)  
#     oc1 = OptionsChain('HSI-%s' % near_expiry)
#     oc1.set_option_structure(contract, 200, 50, 0.0012, 0.0328, near_expiry)
#     oc1.build_chain(19200, 0.08, 0.219)
#     for c in oc1.get_option_chain():
#         print '%s' % ContractHelper.makeRedisKeyEx(c.get_contract())



    
#     oce = OptionsCalculationEngine(config)
# #    oce.add_chain(oc)
#     oce.add_chain(oc1)
#     oce.add_chain(oc2)
#     oce.run_server()
