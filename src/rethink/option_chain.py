# -*- coding: utf-8 -*-
import json
import logging
from ib.ext.Contract import Contract
from misc2.helpers import ContractHelper, dict2str
from finopt.instrument import Symbol, Option
from comms.ibc.base_client_messaging import AbstractGatewayListener
from misc2.observer import Publisher, Subscriber 
from misc2.observer import NotImplementedException


from time import sleep
import finopt.optcal


class OptionsChain(Publisher):
    underlying = None
    spd_size = None
    multiplier = None
    
    #
    # options is a list containing Option object
    options = None
    
    name = None
    div = 0.0
    rate = 0.0
    expiry = None
    trade_vol = None
    #iv = optcal.cal_implvol(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, vol, premium)
    
    
    
    '''
        EVENT_OPTION_UPDATED 
        
        param = {'update_mode': A|D|U <- add/udpate/delete,
                 'name': name_of_this_oc,
                 'instrument: the option associated with this event 
                }
                
        EVENT_UNDERLYING_ADDED
        param = {'update_mode':
                 'name':
                 'instrument': 
                
    '''
    EVENT_OPTION_UPDATED = 'oc_option_updated'
    EVENT_UNDERLYING_ADDED = 'oc_underlying_added'
    OC_EVENTS = [EVENT_OPTION_UPDATED, EVENT_UNDERLYING_ADDED]     
    
    def __init__(self, name):
        self.name = name
        self.options = []
        Publisher.__init__(self, OptionsChain.OC_EVENTS)
        

    def register_listener(self, listener):
        try:
            map(lambda e: self.register(e, listener, getattr(listener, e)), OptionsChain.OC_EVENTS)
        except AttributeError as e:
            logging.error("OptionsChain:add_listener_topics. Function not implemented in the listener. %s" % e)
            raise NotImplementedException        
    
    def get_name(self):
        return self.name
    
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
        self.dispatch(OptionsChain.EVENT_UNDERLYING_ADDED, {'update_mode': 'A', 
                                                            'name': self.name,
                                                            'instrument' : self.get_underlying()}
                      )
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
        self.dispatch(OptionsChain.EVENT_OPTION_UPDATED, {'update_mode': 'A', 
                                                            'name': self.name,
                                                            'instrument' : option}
                      )
    
    
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
        
        # last is 4
        # close is 9
        #fmt_call = map(lambda x: (x[0], '%s,%s,%s,%s,%s,%s,%s,%s' % (format_tick_val(x[1].get_tick_value(9), fmt_spec),
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
                                           format_tick_val(self.get_underlying().get_tick_value(3), fmt_specq)
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
        
     