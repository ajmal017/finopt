import logging
import json
from threading import RLock
from misc2.observer import Publisher
from misc2.observer import NotImplementedException
from misc2.helpers import ContractHelper
from comms.ibc.base_client_messaging import AbstractGatewayListener

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
    EVENT_NEW_SYMBOL_ADDED = 'tds_event_new_symbol_added'
    TDS_EVENTS = [EVENT_TICK_UPDATED, EVENT_NEW_SYMBOL_ADDED] 

    
    def __init__(self, name):
        

        self.tickers = {}
        self.symbols = {}
        self.lock = RLock()
        Publisher.__init__(self, TickDataStore.TDS_EVENTS)
        self.first_run = True
        
        
    def register_listener(self, l):
        map(lambda e: self.register(e, l, getattr(l, e)), TickDataStore.TDS_EVENTS)

    def dump(self):
            # print ', '.join('[%s:%s]' % (k, v['ticker_id'])) 
        logging.info('TickDataStore-symbols: [Key: Ticker ID: # options objects]: ---->\n%s' % (',\n'.join('[%s:%d:%d]' % (k, v['ticker_id'], len(v['syms'])) for k, v in self.symbols.iteritems())))
        logging.info('TickDataStore-tickers: %s' % self.tickers)
    
     
    
    def add_symbol(self, symbol):
        try:
            self.lock.acquire()
            key = symbol.get_key()
            if key not in self.symbols:
                self.symbols[key] = {'ticker_id':-1, 'syms': []}
                
            self.symbols[key]['syms'].append(symbol)        
    
            # defer the dispatch at the end of this method        
            if key not in self.symbols:
                self.dispatch(TickDataStore.EVENT_NEW_SYMBOL_ADDED, symbol)
        finally:
            self.lock.release()
            
    def del_symbol(self, symbol):
        raise NotImplementedException     
    
       
        
    def set_symbol_price(self, event, message_value):   
        
        # 'value': '{"tickerId": 0, "size": 3, "field": 3}'
        items = json.loads(message_value)
        tid = items['tickerId']

        try:
            self.lock.acquire()
            contract_key = self.tickers[tid]
            # print contract_key
            map(lambda e: e.set_tick_value(items['field'], items['price']), self.symbols[contract_key]['syms'])
            
        except KeyError:
            # contract not set up in the datastore, ignore message
            pass
        finally:
            self.lock.release()
            self.dispatch(TickDataStore.EVENT_TICK_UPDATED, message_value)
            


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
        def set_values2(idc):
            
            
            key = ContractHelper.makeRedisKeyEx(idc[1])
            
            if key in self.symbols and idc[0] <> self.symbols[key]['ticker_id']:
                # if this condition is met, one should delete the old entry
                # and move all object references to the new key/ticker_id
                raise
            
            self.tickers[idc[0]] = key
            try:
                self.symbols[key]['ticker_id'] = idc[0]
            except KeyError:
                self.symbols[key] = {'ticker_id': idc[0],
                                       'syms': []}
                

        
        try:
            def utf2asc(x):
                return x if isinstance(x, unicode) else x
        
            self.lock.acquire()
            
            items = json.loads(subscription_message_value['value'])
            logging.info('TickDataStore:update_datastore. items: %s ' % items)
            id_contracts = map(lambda x: (x[0], ContractHelper.kvstring2contract(utf2asc(x[1]))), items['subscriptions'])
            map(lambda idc: set_values2, id_contracts)   
            self.dump()
        except TypeError:
            logging.error('TickDataStore:gw_subscriptions. Exception when trying to get id:contracts.')
            return None       
        finally:
            self.lock.release()     
        
            



        
        
        
