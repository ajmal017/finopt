# -*- coding: utf-8 -*-

import sys
import ConfigParser
import json
import logging
import threading
from ib.ext.Contract import Contract
from misc2.helpers import ContractHelper, dict2str
from instrument import Symbol, Option
from misc2.observer import Publisher, Subscriber 
#from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
from comms.tws_client import SimpleTWSClient
from time import sleep
import time, datetime
import optcal
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
        
class OptionsCalculationEngine(SimpleTWSClient):
    tickerMap = {}
    option_chains = {}    
    appid = 'OCE'
    
    download_gw_map_done = False
    rs = None
    rs_oc_prefix = None
    
    
    """
        symbols= {}
        tickers={}
        optionChain->id, [optsym1,optsym2]
        
        
        
        symbols[symbol_key]={'ticker_id': None, 'syms': [symbol1, symbol2...symbolN]}
        tickers[ticker_id]=symbol_key
        
        
        oce = OptionCalEngine('OCE')
        oce.start()
        
        oc = OptionChain('QQQ'）
        oce.register(oc) # listen for oc events
        
        
        o1 = Option(ContractHelper.makeContract(contractTuple))
        oc.add_option(o1) --> notify listeners' update method
        
        #oce
        def update(method, option):
            key = ContractHelper.makeRedisKeyEx(option)
            symbols[key]['ticker_id'] = -1
        
            symbols[key]['syms'].append(option)
        
        
        def gw_subscription_changed(param):
            ticker_id = param[ticker_id]
            symbol_key= param[symbol_key]
            symbols[key]['ticker_id'] = ticker_id
            tickers[ticker_id] = key
        
        def tick_price(vars):
        
            key= tickers[vars['id']]
            for o in symbols[key]['syms']:
                # update message values 
    
    """
    
    
    
    
    
    def __init__(self, config): #host, port, id=None):
        
        
        khost = config.get("epc", "kafka.host").strip('"').strip("'")
        kport = config.get("epc", "kafka.port")
        
             
        super(OptionsCalculationEngine, self).__init__(khost, kport, self.appid)
        logging.info('OptionsCalculationEngine client id=%s' % id)    
        
        self.initialize_redis(config)

    def initialize_redis(self, config):
        r_host = config.get("redis", "redis.server").strip('"').strip("'")
        r_port = config.get("redis", "redis.port")
        r_db = config.get("redis", "redis.db")     
        clr_flag = config.get("options_chain", "clear_redis_on_start")
        
        self.rs = redis.Redis(r_host, r_port, r_db)
        try:
            if clr_flag.upper() == 'TRUE':
                
                logging.info('clear previously saved option chain values in redis...')
                # clear previously saved option chain values
                self.rs_oc_prefix = config.get("options_chain", "option_chain_id.redis_key_prefix").strip('"').strip("'")
                # find all chains with keys beginning like rs_oc_prefix
                oc_sets = self.rs.keys(pattern = '%s*' % self.rs_oc_prefix)
                
                for oc_set in oc_sets:
                    l = map(lambda x: self.rs.srem(oc_set, x), list(self.rs.smembers(oc_set)))
                    self.rs.delete(oc_set)
                
            
            #self.rs.client_list()
        except redis.ConnectionError:
            logging.error('TWS_gateway: unable to connect to redis server using these settings: %s port:%d db:%d' % (r_host, r_port, r_db))
            logging.error('aborting...')
            sys.exit(-1)
    
    def add_chain(self, option_chain):
        self.option_chains[option_chain.get_id()] = option_chain
    


        
    def map_gw_ticker_to_option_chains(self):

        for k, option_chain in self.option_chains.iteritems():
            for option in option_chain.get_option_chain():
                self.get_command_handler().reqMktData(option.get_contract())
            self.get_command_handler().reqMktData(option_chain.get_underlying().get_contract())


        
        self.get_command_handler().gw_req_subscriptions()
        while not self.download_gw_map_done:
            sleep(1)
            logging.info('map_gw_ticker_to_option_chains: awaiting subscription table down to get done...')
            pass
        
        logging.info('map_gw_ticker_to_option_chains: complete download subscription table from gw')
        
        # for each tick id in the tickerMap, compare...
        for tickerId, cv in self.tickerMap.iteritems():
            
            # for each chain in the option chain, iterate...
            for chain_id, chain in self.option_chains.iteritems():
                
                # for each of the item (Option object) within the option chain...
                for i in range(len(chain.get_option_chain())):
                    
                    # if a match is found...                 
                    if cv['contract'] == chain.get_option_chain()[i].get_contract():
                        print 'id:%s col:%d -> tickerId: %d >> %s %s' % (chain_id, i, tickerId, ContractHelper.makeRedisKeyEx(cv['contract']),\
                                                                          ContractHelper.makeRedisKeyEx(chain.get_option_chain()[i].get_contract()))
                        
                        # update the ticker map
                        # key= tws ticker id
                        # value-> key: tick2oc_slot (ticker map to option chain slot) => [chain_id, the ith element]
                        cv['tick2oc_slot'] = [chain_id, i]
                        
                if chain.get_underlying().get_contract() == cv['contract']:
                        print 'id:%s col:%d -> tickerId: %d >> %s %s' % (chain_id, i, tickerId, ContractHelper.makeRedisKeyEx(cv['contract']),\
                                                                          ContractHelper.makeRedisKeyEx(chain.get_underlying().get_contract()))
                        cv['tick2oc_slot'] = [chain_id, -999]
        

    
    def get_option_in_chain(self, chain_id, elem_at):
        return self.option_chains[chain_id].get_option_chain()[elem_at]
    
    def get_underlying_in_chain(self, chain_id):
        return self.option_chains[chain_id].get_underlying()
    
    def run_server(self):
        self.connect()
            
        self.map_gw_ticker_to_option_chains()
        
        #sleep(5)
        while 1:
            sleep(5)
            for oc in self.option_chains.keys():
                #self.option_chains[oc].pretty_print()
                
                self.option_chains[oc].generate_google_datatable_json()
            
        self.disconnect()
        
        
        
######################################################
#   TWS messages
    def dump(self, msg_name, mapping):
        # the mapping is a comms.tws_protocol_helper.Message object
        # which can be accessed directly using the __dict__.['xxx'] method 
        items = list(mapping.items())
        items.sort()
        print ('>>> %s <<< %s' % (msg_name, ''.join('%s=%s, '% (k, v if k <> 'ts' else datetime.datetime.fromtimestamp(v).strftime('%Y-%m-%d %H:%M:%S.%f')) for k, v in items))) 
    
        
    # override the tickSize message
    def tickSize(self, items):
    
        try:
            contract = self.tickerMap[items.__dict__['tickerId']]['contract']
            tick2oc_slot = self.tickerMap[items.__dict__['tickerId']]['tick2oc_slot']
            field = items.__dict__['field']
            
            logging.debug('tickSize>> %s' % ('[%d:%s:%s:%d] %s=%d %0.2f [%s]' % \
                                        (items.__dict__['tickerId'], ContractHelper.makeRedisKeyEx(contract),\
                                         tick2oc_slot[0], tick2oc_slot[1],\
                                        'bid_q' if field == 0 else ('ask_q' if field == 3 else ('last_q' if field == 5 else field)), \
                                        items.__dict__['size'], self.option_chains[tick2oc_slot[0]].multiplier,\
                                        datetime.datetime.fromtimestamp(items.__dict__['ts']).strftime('%Y-%m-%d %H:%M:%S.%f')))
                          )
            # is an option
            if tick2oc_slot[1] <> -999:
                o = self.get_option_in_chain(tick2oc_slot[0], tick2oc_slot[1])
                o.set_tick_value(field, items.__dict__['size'])

            # is an underylying  
            else:             
                
                self.get_underlying_in_chain(tick2oc_slot[0]).set_tick_value(field, items.__dict__['size'])
                #print 'set fut price %s %d:%0.2f' % (ContractHelper.makeRedisKeyEx(self.get_underlying_in_chain(tick2oc_slot[0]).get_contract()), field, items.__dict__['price'])
            
            
        except KeyError:
            logging.error('tickSize: keyerror: (this could happen on the 1st run as the subscription manager sub list is still empty.')
            logging.error(''.join('%s=%s, '% (k,v) for k,v in items.__dict__.iteritems()))




    def tickPrice(self, items):
        try:
            contract = self.tickerMap[items.__dict__['tickerId']]['contract']
            field = items.__dict__['field']
            tick2oc_slot = self.tickerMap[items.__dict__['tickerId']]['tick2oc_slot']
            today = time.strftime('%Y%m%d')
            price = items.__dict__['price']
            
            #
            # perform some sanity check
            # 
            # if field is not bid, ask, last, or close, pass
            if field not in [1,2,4,9]:
                logging.debug('tickPrice: discard unwanted msg field:%d' % field)
                return
            
            # if we received a negative price, pass
            if price == -1:
                logging.debug('tickPrice: discard unwanted msg price==-1')
                return 
        
            logging.debug( 'tickPrice>> %s' % ('[%d:%s:%s:%d] %s=%0.4f [%s]' % \
                                        (items.__dict__['tickerId'], ContractHelper.makeRedisKeyEx(contract),\
                                         tick2oc_slot[0], tick2oc_slot[1],\
                                        'bid' if field == 1 else ('ask' if field == 2 else ('last' if field == 4 else field)), \
                                        items.__dict__['price'], datetime.datetime.fromtimestamp(items.__dict__['ts']).strftime('%Y-%m-%d %H:%M:%S.%f')))
                          )
        
            # is an option
            if tick2oc_slot[1] <> -999:
                o = self.get_option_in_chain(tick2oc_slot[0], tick2oc_slot[1])
                o.set_tick_value(field, items.__dict__['price'])

                try:
			
                    spot = self.get_underlying_in_chain(tick2oc_slot[0]).get_tick_value(4)
					   
			
                    
                    # the underlying price may not be available when we receive tick price for options
                    if spot <> None:
                        
                    
                        rate = self.option_chains[tick2oc_slot[0]].rate
                        div = self.option_chains[tick2oc_slot[0]].div
                        tvol = self.option_chains[tick2oc_slot[0]].trade_vol
                        logging.debug('sp=%0.4f, x=%0.4f, %s, evald=%s, expiryd=%s, r=%0.4f, d=%0.4f, v=%0.4f, px[%d]=%0.4f' % (\
                                                spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate,\
                                                div, tvol, field, items.__dict__['price']))
                        
                        results = None                  
                        iv = optcal.cal_implvol(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate,\
                                                div, tvol, items.__dict__['price'])
                        results = optcal.cal_option(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, iv['imvol'])
                        results[Option.IMPL_VOL] = iv['imvol']
                        #print results
                        o.set_analytics(**results)
                        logging.debug(o.get_analytics())
                        o.set_extra_attributes('spot', spot)
                        o.set_extra_attributes('rate', rate)
                        o.set_extra_attributes('div', div)   
                        o.set_extra_attributes('chain_id', tick2oc_slot[0])
                      
                                  
                except Exception, err:
                    logging.error(traceback.format_exc())

                

                o.set_extra_attributes('last_updated', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
                self.broadcast_analytics(tick2oc_slot[0], o)
                logging.debug(o.object2kvstring())
            # is an underylying  
            else:             
                
                self.get_underlying_in_chain(tick2oc_slot[0]).set_tick_value(field, items.__dict__['price'])
                #print 'set fut price %s %d:%0.2f' % (ContractHelper.makeRedisKeyEx(self.get_underlying_in_chain(tick2oc_slot[0]).get_contract()), field, items.__dict__['price'])
            
        except KeyError:
            logging.error('tickPrice: keyerror: (this could happen on the 1st run as the subscription manager sub list is still empty.')
            logging.error(''.join('%s=%s, '% (k,v) for k,v in items.__dict__.iteritems()))
            
            
    def tickString(self, items):
        pass

    def tickGeneric(self, items):
        pass

    def tickSnapshotEnd(self, items):
        pass

    def error(self, items):
        self.dump('error', items)

    def error_0(self, items):
        self.dump('error', items)
 
    def error_1(self, items):
        self.dump('error', items)



######################################################
#   GW messages
          
    def gw_subscriptions(self, items):
        # <class 'comms.tws_protocol_helper.Message'>
        # sample
        #{0: {'contract': <ib.ext.Contract.Contract object at 0x7ff8f8c9e210>}, 1: {'contract': <ib.ext.Contract.Contract object at 0x7ff8f8c9e250>},... }
        #print items.__dict__['subscriptions']
        
        l = map(lambda x: {x[0]: {'contract': x[1]}}, map(lambda x: (x[0], ContractHelper.kvstring2object(x[1], Contract)), items.__dict__['subscriptions']))
        #l = map(lambda x: {x[0]: x[1]}, map(lambda x: (x[0], json.loads(x[1])), items.__dict__['subscriptions']))
        for i in l:
            self.tickerMap.update(i)   
        logging.info('gw_subscriptions -> dump tickerMap ')
        logging.info(''.join('%s=%s,' % (k,ContractHelper.makeRedisKeyEx(v['contract'])) for k,v in self.tickerMap.iteritems())) 
    
        self.download_gw_map_done = True

    
    
    def broadcast_analytics(self, chain_id, option):
        
        o_key = ContractHelper.makeRedisKeyEx(option.get_contract())
        self.rs.sadd('%s%s' % (self.rs_oc_prefix, chain_id), o_key)
        
        msg_str = option.object2kvstring()
        self.rs.set(o_key, msg_str)
        
        
        self.get_producer().send_messages('optionAnalytics', msg_str)
    
    #def broadcast_chain_snapshots(self):
        


class OCConsumer(Subscriber):
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
    
        
    def tickPrice(self, items):   
        
        tid = items['tickerId']
        #print tid
        if tid in self.tickers:
            contract_key = self.tickers[tid]
            #print contract_key
            for e in self.symbols[contract_key]['syms']:
                e.set_tick_value(items['field'], items['price'])
            
        
    def gw_subscription_changed(self, items):
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
        #l = map(lambda x: {x[0]: {'contract': x[1]}}, map(lambda x: (x[0], ContractHelper.kvstring2object(x[1], Contract)), items)) #items.__dict__['subscriptions']))
        #l = map(lambda x: {x[0]: x[1]}, map(lambda x: (x[0], json.loads(x[1])), items.__dict__['subscriptions']))

        for tid, con in items.iteritems():
            contract = ContractHelper.kv2contract(con)
            key = ContractHelper.makeRedisKeyEx(contract)
            if key in self.symbols: 
                self.symbols[key]['ticker_id'] = tid
                self.tickers[tid] = key
        
        
                
        
         
              
              
              
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
