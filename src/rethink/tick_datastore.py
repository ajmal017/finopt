import logging
import json
from threading import RLock
from misc2.observer import Publisher
from misc2.observer import NotImplementedException
from misc2.helpers import ContractHelper
from comms.ibc.base_client_messaging import AbstractGatewayListener
import symbol, traceback

class TickDataStore(Publisher):
    """
    
    Data structure:

    
    
    """
    
    '''
        EVENT_TICK_UPDATED 
        
        param = {'update_mode': A|D|U <- add/udpate/delete,
                 'name': name_of_this_oc,
                 'instrument: the option associated with this event 
                }
                
        EVENT_UNDERLYING_ADDED
        param = {'update_mode':
                 'name':
                 'instrument': 
                
    '''
    EVENT_TICK_UPDATED = 'tds_event_tick_updated'
    EVENT_SYMBOL_ADDED = 'tds_event_symbol_added'
    EVENT_SYMBOL_DELETED = 'tds_event_symbol_deleted'
    TDS_EVENTS = [EVENT_TICK_UPDATED, EVENT_SYMBOL_ADDED, EVENT_SYMBOL_DELETED] 

    
    def __init__(self, name):
        
        self.symbols = {}
        self.name = name
        
        self.lock = RLock()
        Publisher.__init__(self, TickDataStore.TDS_EVENTS)
        self.first_run = True
        
        
    def register_listener(self, listener):
        
        try:
            map(lambda e: self.register(e, listener, getattr(listener, e)), TickDataStore.TDS_EVENTS)
        except AttributeError as e:
            logging.error("TickDataStore:register_listener. Function not implemented in the listener. %s" % e)
            raise NotImplementedException        
        

    def dump(self):
    
        
        def format_tick_val(val, fmt):
            if val == None:
                length = len(fmt % (0))
                return ' ' * length
            
            return fmt % (val) 
        
        # last, bidq, bid, ask, askq, imvol, delta, theta
        fmt_spec = '%8.2f'
        fmt_spec2 = '%8.4f'
        fmt_specq = '%8d'
        
        
        def get_field(sym, fld_id):
            try:
                return sym[0].get_tick_value(fld_id)
            except:
                return ''

        
        fmt_sym = map(lambda x: (x[0], '%s,%s,%s,%s,%s,%s' % (
                                            format_tick_val(get_field(x[1]['syms'],4), fmt_spec),
                                            format_tick_val(get_field(x[1]['syms'],0), fmt_specq),                                                                                                                  
                                            format_tick_val(get_field(x[1]['syms'],1), fmt_spec),
                                            format_tick_val(get_field(x[1]['syms'],2), fmt_spec), 
                                            format_tick_val(get_field(x[1]['syms'],3), fmt_specq),
                                            format_tick_val(get_field(x[1]['syms'],9), fmt_spec),
                                            )), [(k,v) for k, v in self.symbols.iteritems()])        
        

        for e in fmt_sym:
            print('[%s]%s' % (e[0].ljust(40), e[1]))

    def is_symbol_in_list(self, symbol, list):
    
        for s in list:
            if s is symbol:
                return True
        
        return False
    
    
    
    def add_symbol(self, symbol):
        try:
            dispatch = True
            self.lock.acquire()
            key = symbol.get_key()
            if key not in self.symbols:
                self.symbols[key] = {'syms': [symbol]}

            else:
                if not self.is_symbol_in_list(symbol, self.symbols[key]['syms']): 
                    self.symbols[key]['syms'].append(symbol)        
    
        except KeyError:
            dispatch = False
            logging.error('TickDataStore: add_symbol. Exception when adding symbol:%s' % key)
        finally:            
            self.lock.release()        
            if dispatch:
                self.dispatch(TickDataStore.EVENT_SYMBOL_ADDED, {'update_mode': 'A', 
                                                            'name': self.name,
                                                            'instrument' : symbol})
            
            
    def del_symbol(self, symbol):
           
        try:
            dispatch = True
            self.lock.acquire()
            key = symbol.get_key()
            if key not in self.symbols:
                return
            else:
                for s in self.symbols[key]['syms']:
                    if s is symbol:
                        self.symbols[key]['syms'].remove(s)
                    
        except KeyError:
            dispatch = False
            logging.error('TickDataStore: del_symbol. Exception when deleting symbol:%s' % key)
        finally:            
            self.lock.release()   
            if dispatch:
               self.dispatch(TickDataStore.EVENT_SYMBOL_DELETED,  {'update_mode': 'D', 
                                                            'name': self.name,
                                                            'instrument' : symbol})                 
                                    
    
       
        
    def set_symbol_tick_price(self, contract_key, field, price, canAutoExecute):   
        logging.debug('set_symbol_price: -------------------')
        try:
            self.lock.acquire()
            if contract_key in self.symbols:
                logging.debug('set_symbol_tick_price: ***** sym key= : %s' % contract_key)
                logging.debug('set_symbol_tick_price: ***** sym= : %s' % str(self.symbols[contract_key]['syms']))
                
                map(lambda e: e.set_tick_value(field, price), self.symbols[contract_key]['syms'])
                
                self.dispatch(TickDataStore.EVENT_TICK_UPDATED, {'contract_key': contract_key, 'field': field, 
                                                             'price': price, 'syms': self.symbols[contract_key]['syms']})                
                
                
        except:
            # contract not set up in the datastore, ignore message
            logging.error('tick_datastore:set_symbol_tick_price: exception occured to: %s. Exception could have been triggered due to the dispatched client processing logic' % contract_key)
            logging.error(traceback.format_exc())
            #self.dump()
            pass
        finally:
            self.lock.release()

            

    def set_symbol_analytics(self, contract_key, field, value):
        logging.debug('set_symbol_analytics: -------------------')
        try:
            self.lock.acquire()
            if contract_key in self.symbols:
                map(lambda e: e.set_tick_value(field, value), self.symbols[contract_key]['syms'])
            
        except:
            # contract not set up in the datastore, ignore message
            logging.error('set_symbol_price: exception occured to: %s' % contract_key)
            #self.dump()
            pass
        finally:
            self.lock.release()

        
    def set_symbol_tick_size(self, contract_key, field, size): 
        
  
        
        logging.debug('set_symbol_size: -------------------')
        try:
            self.lock.acquire()
            if contract_key in self.symbols:
                map(lambda e: e.set_tick_value(field, size), self.symbols[contract_key]['syms'])
            
        except:
            # contract not set up in the datastore, ignore message
            logging.error('set_symbol_size: exception occured to: %s' % contract_key)
            #self.dump()
            pass
        finally:
            self.lock.release()
            #self.dispatch(TickDataStore.EVENT_TICK_UPDATED, {'contract_key': contract_key, 'field': field, 
            #                                                 'size': size})
            
        




        
        
        
