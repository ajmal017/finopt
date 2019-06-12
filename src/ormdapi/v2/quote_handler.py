import logging
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from copy import deepcopy
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
        self.sub_mgr = gw_parent.get_subscription_manager()
        
        Subscriber.__init__(self, self.name)
        
        '''
             ask tws_event_handler to forward tick messages to
             this class
             
        '''
        for e in ['tickPrice', 'tickSize', 'tickOptionComputation', 'error']:
            self.tws_event_handler.register(e, self)           
        

    def handle_tickprice(self, contract_key, field, price, canAutoExecute):
        logging.debug('QuoteHandler:tickPrice')
        try:
            s = self.symbols[contract_key]
            '''
                a contract may have already existed in the self.symbols dictionary
                from a previous subscription attempt that was unsuccessful,
                resulting in no Symbol created but an error message stored
                as a dict and assigned to self.symbols (check the code in update
                function below)
                the following if condition deals with such a case by recreating a
                new symbol and getting rid of the error dict
            '''
            if not isinstance(s, Symbol):
                raise KeyError            
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(field, price)
            
            
        
    
    def handle_ticksize(self, contract_key, field, size):
        logging.info('QuoteHandler:ticksize %s' % contract_key)
        try:
            s = self.symbols[contract_key]
            if not isinstance(s, Symbol):
                raise KeyError
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(field, size)

    def handle_tickgreeks(self, **params):
        logging.debug('QuoteHandler:tickOptionComputation')
        try:
            contract_key = params['contract_key']
            s = self.symbols[contract_key]
            if not isinstance(s, Symbol):
                raise KeyError            
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        greeks = deepcopy(params)
        del greeks['greeks']['field']
        del greeks['contract_key']
        s.set_ib_option_greeks(params['greeks']['field'], greeks['greeks'])      
        
    def update(self, event, **param): 
        if event == 'tickPrice':
            self.handle_tickprice(**param)
        elif event == 'tickSize':
            self.handle_ticksize(**param)
        elif event == 'tickOptionComputation':
            self.handle_tickgreeks(**param)
        elif event == 'error':            
            try:
                
                if param['id'] >= 1000 and param['id'] < 1999:
                    ckey = self.sub_mgr.get_contract_key_by_id(param['id']-1000)
                    self.symbols[ckey] = {'error': param['errorMsg']}
                    
            except:
                pass
            
    
    def get_symbol(self, contract):
        try:
            return self.symbols[ContractHelper.makeRedisKeyEx(contract)]
        except KeyError:
            return None
        

