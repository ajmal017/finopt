import logging
import json
from threading import RLock
from misc2.observer import Subscriber
from misc2.observer import NotImplementedException
from misc2.helpers import ContractHelper

import traceback
from finopt.instrument import Symbol

class QuoteRESTHandler(Subscriber):


    '''
    
        self.symbols = {contract_key: Symbol, ...}
        
    '''
    
    def __init__(self, name, gw_parent):
        
        self.symbols = {}
        self.name = name
        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        
        Subscriber.__init__(self, self.name)
        
        '''
             ask tws_event_handler to forward tick messages to
             this class
             
        '''
        for e in ['tickPrice', 'tickSize']:
            self.tws_event_handler.register(e, self)           
        

    def handle_tickprice(self, contract_key, field, price, canAutoExecute):
        logging.debug('QuoteHandler:tickPrice')
        try:
            s = self.symbols[contract_key]
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(field, price)
            
            
        
    
    def handle_ticksize(self, contract_key, field, size):
        logging.debug('QuoteHandler:ticksize')
        try:
            s = self.symbols[contract_key]
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(field, size)

        
    def update(self, event, **param): 
        if event == 'tickPrice':
            self.handle_tickprice(**param)
        elif event == 'tickSize':
            self.handle_ticksize(**param)
        
    
    def get_symbol_ticks(self, contract):
        try:
            return self.symbols[ContractHelper.makeRedisKeyEx(contract)]
        except KeyError:
            return None
        
        
    def dump(self):
    
        
        def format_tick_val(val, fmt):
            if val == None:
                length = len(fmt % (0))
                return ' ' * length
            
            return fmt % (val) 
        
        
        fmt_spec = '%8.2f'
        fmt_spec2 = '%8.4f'
        fmt_specq = '%8d'
        
        
        def get_field(sym, fld_id):
            try:
                return sym.get_tick_value(fld_id)
            except:
                return ''

        
        
        fmt_sym = map(lambda x: (x[0], '%s,%s,%s,%s,%s,%s,%s' % (
                                            format_tick_val(get_field(x[1],Symbol.LAST), fmt_spec),
                                            format_tick_val(get_field(x[1],Symbol.BIDSIZE), fmt_specq),                                                                                                                  
                                            format_tick_val(get_field(x[1],Symbol.BID), fmt_spec),
                                            format_tick_val(get_field(x[1],Symbol.ASK), fmt_spec), 
                                            format_tick_val(get_field(x[1],Symbol.ASKSIZE), fmt_specq),
                                            format_tick_val(get_field(x[1],Symbol.CLOSE), fmt_spec),
                                            format_tick_val(get_field(x[1],Symbol.VOLUME), fmt_specq),
                                            )), [(k,v) for k, v in self.symbols.iteritems()])        
        
        print('%40s,%8s,%8s,%8s,%8s,%8s,%8s,%8s\n' % ('SYM', 'LAST', 'BIDSIZE','BID','ASK','ASKSIZE','CLOSE','VOLUME'
                                             ))

        for e in fmt_sym:
            print('[%s]%s' % (e[0].ljust(40), e[1]))

    
    
        




        
        
        
