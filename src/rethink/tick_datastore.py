import logging
import json
from threading import RLock
from misc2.observer import Publisher
from misc2.observer import NotImplementedException
from misc2.helpers import ContractHelper
from comms.ibc.base_client_messaging import AbstractGatewayListener
from numpy import disp
import symbol

class TickDataStore(Publisher):
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
    

    EVENT_TICK_UPDATED = 'tds_event_tick_updated'
    EVENT_SYMBOL_ADDED = 'tds_event_symbol_added'
    EVENT_SYMBOL_DELETED = 'tds_event_symbol_deleted'
    TDS_EVENTS = [EVENT_TICK_UPDATED, EVENT_SYMBOL_ADDED, EVENT_SYMBOL_DELETED] 

    
    def __init__(self, name):
        
        self.symbols = {}
        
        
        self.lock = RLock()
        Publisher.__init__(self, TickDataStore.TDS_EVENTS)
        self.first_run = True
        
        
    def register_listener(self, l):
        map(lambda e: self.register(e, l, getattr(l, e)), TickDataStore.TDS_EVENTS)

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

        
        fmt_sym = map(lambda x: (x[0], '%s,%s,%s,%s,%s' % (
                                            format_tick_val(get_field(x[1]['syms'],4), fmt_spec),
                                            format_tick_val(get_field(x[1]['syms'],0), fmt_specq),                                                                                                                  
                                            format_tick_val(get_field(x[1]['syms'],1), fmt_spec),
                                            format_tick_val(get_field(x[1]['syms'],2), fmt_spec), 
                                            format_tick_val(get_field(x[1]['syms'],3), fmt_specq),
                                            
                                            )), [(k,v) for k, v in self.symbols.iteritems()])        
        

        for e in fmt_sym:
            print('[%s]%s' % (e[0].ljust(50), e[1]))

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
                self.dispatch(TickDataStore.EVENT_SYMBOL_ADDED, symbol)
            
            
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
               self.dispatch(TickDataStore.EVENT_SYMBOL_DELETED, symbol)                 
                                    
    
       
        
    def set_symbol_tick_price(self, contract_key, field, price, canAutoExecute):   
        
        # message_value: dict: '{"tickerId": 0, "size": 3, "field": 3}'
        
        
        
        logging.debug('set_symbol_price: -------------------')
        try:
            self.lock.acquire()
            if contract_key in self.symbols:
                map(lambda e: e.set_tick_value(field, price), self.symbols[contract_key]['syms'])
            
        except:
            # contract not set up in the datastore, ignore message
            logging.error('set_symbol_price: exception occured to: %s' % contract_key)
            #self.dump()
            pass
        finally:
            self.lock.release()
            self.dispatch(TickDataStore.EVENT_TICK_UPDATED, vars())
            


    def update_datastore(self, subscription_message_value):
        '''
        sample value:
        {
        'partition': 0, 'value': '{"target_id": "analytics_engine", "sender_id": "tws_gateway_server", 
        "subscriptions": [[0, "{\\"m_conId\\": 0, \\"m_right\\": \\"\\", \\"m_symbol\\": \\"HSI\\", \\"m_secType\\": \\"FUT\\", 
        \\"m_includeExpired\\": false, \\"m_expiry\\": \\"20170330\\", \\"m_currency\\": \\"HKD\\", \\"m_exchange\\": \\"HKFE\\", \\"m_strike\\": 0}"]]}', 
        'offset': 13
        }
        '''
        def set_datastore_values(idc):
            
            
            key = ContractHelper.makeRedisKeyEx(idc[1])
            if key in self.symbols and idc[0] <> self.symbols[key]['ticker_id']:
                # if this condition is met, one should delete the old entry
                # and move all object references to the new key/ticker_id
                if self.symbols[key]['ticker_id'] <> -1:
                    raise
            
            self.tickers[idc[0]] = key
            try:
                self.symbols[key]['ticker_id'] = idc[0]
                
            except KeyError:
                self.symbols[key] = {'ticker_id': idc[0], 'syms': []}
            
            self.dump()
            return key
                

        
        try:
            def utf2asc(x):
                return x if isinstance(x, unicode) else x
        
            self.lock.acquire()
            
            items = json.loads(subscription_message_value['value'])
            logging.info('TickDataStore:update_datastore. items: %s ' % items)
            id_contracts = map(lambda x: (x[0], ContractHelper.kvstring2contract(utf2asc(x[1]))), items['subscriptions'])
            map(set_datastore_values, id_contracts)
            self.dump()
        except TypeError:
            logging.error('TickDataStore:gw_subscriptions. Exception when trying to get id:contracts.')
            return None       
        finally:
            self.lock.release()     
        
            



        
        
        
