# -*- coding: utf-8 -*-
import sys, traceback
import logging
import json
import math
import time, datetime
import copy
from optparse import OptionParser
from time import sleep
from misc2.helpers import ContractHelper
from finopt.instrument import Symbol, Option
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from numpy import average
from rethink.table_model import AbstractTableModel
from gtk.keysyms import percent

class PortfolioRules():
    rule_map = {
                'symbol': {'HSI' : 'FUT', 'MHI' : 'FUT', 'QQQ' : 'STK'},
                'expiry': {'HSI' : 'same_month', 'MHI': 'same_month', 'STK': 'leave_blank'},
                'option_structure': {
                                        'HSI': {'spd_size': 200, 'multiplier': 50, 'rate': 0.0012, 'div': 0, 'trade_vol':0.15},
                                        'MHI': {'spd_size': 200, 'multiplier': 10, 'rate': 0.0012, 'div': 0, 'trade_vol':0.15}
                                        
                                    },
                'exchange': {'HSI': 'HKFE', 'MHI': 'HKFE'},              
                'interested_position_types': {'symbol': ['HSI', 'MHI'], 'instrument_type': ['OPT', 'FUT']}

               } 
    
class PortfolioItem():
    """
    
    """
    POSITION = 7001
    AVERAGE_COST = 7002
    POSITION_DELTA = 7003
    POSITION_THETA = 7004
    GAMMA_PERCENT = 7009
    UNREAL_PL = 7005
    PERCENT_GAIN_LOSS = 7006
    AVERAGE_PRICE = 7007
    MARKET_VALUE = 7008
    
        
    def __init__(self, account, contract_key, position, average_cost):
        
        self.contract_key = contract_key
        self.account_id = account
        self.port_fields = {PortfolioItem.POSITION: position,
                            PortfolioItem.AVERAGE_COST: average_cost,
                            PortfolioItem.POSITION_DELTA: float('nan'),
                            PortfolioItem.POSITION_THETA: float('nan'),
                            PortfolioItem.UNREAL_PL: float('nan'),
                            PortfolioItem.PERCENT_GAIN_LOSS: float('nan'),
                            PortfolioItem.AVERAGE_PRICE: float('nan'),
                            PortfolioItem.MARKET_VALUE: float('nan')
                            
                            }
        
        contract = ContractHelper.makeContractfromRedisKeyEx(contract_key)
        
        
        if contract.m_secType == 'OPT':
            self.instrument = Option(contract)
        else: 
            self.instrument = Symbol(contract)
        

    
    def set_port_field(self, id, value):
        self.port_fields[id] = value

    def get_port_field(self, id):
        try:
            
            return self.port_fields[id]
    
        except:
            
            return None    
    def get_port_fields(self):
        return self.port_fields
    
    def get_contract_key(self):
        return self.contract_key
    
    def get_right(self):
        return self.instrument.get_contract().m_right
    
    def get_symbol_id(self):
        return self.instrument.get_contract().m_symbol
    
    def get_expiry(self):
        return self.instrument.get_contract().m_expiry
    
    def get_strike(self):
        return self.instrument.get_contract().m_strike
    
    def get_quantity(self):
        return self.port_fields[PortfolioItem.POSITION]
    
    def get_average_cost(self):
        return self.port_fields[PortfolioItem.AVERAGE_COST]
    
    def get_market_value(self):
        return self.port_fields[PortfolioItem.MARKET_VALUE]
    
    def get_instrument(self):
        return self.instrument
        
    def get_instrument_type(self):
        return self.instrument.get_contract().m_secType
    
    def get_account(self):
        return self.account_id
        
    def calculate_pl(self, contract_key):
        
        #logging.info('PortfolioItem:calculate_pl. %s' % self.dump())
        '''
            POSITION = 7001
            AVERAGE_COST = 7002
            POSITION_DELTA = 7003
            POSITION_THETA = 7004
            UNREAL_PL = 7005
            PERCENT_GAIN_LOSS = 7006
            AVERAGE_PRICE = 7007
            MARKET_VALUE = 7008            
        '''
            
    
        try:
            assert contract_key == self.contract_key
            spot_px = self.instrument.get_tick_value(4)
            if self.get_instrument_type() == 'OPT':
                #spot_px = self.instrument.get_tick_value(4)
                multiplier =  PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier']
                
                pos_delta = self.get_quantity() * self.instrument.get_tick_value(Option.DELTA) * multiplier                                
                pos_theta = self.get_quantity() * self.instrument.get_tick_value(Option.THETA) * multiplier
                gamma_percent = pos_delta * (1 + self.instrument.get_tick_value(Option.GAMMA))                               

                #(spot premium * multiplier - avgcost) * pos)
                try:
                    unreal_pl = (spot_px * multiplier - self.get_average_cost()) * self.get_quantity()
                    #print "%f %f %d" % (spot_px, self.get_average_cost(), multiplier)
                    percent_gain_loss = (1 - spot_px / (self.get_average_cost() / multiplier)) * 100 \
                                            if self.get_quantity() < 0 else \
                                            (spot_px - self.get_average_cost() / multiplier) / (self.get_average_cost() / multiplier) * 100 
                                        
                    average_px = self.get_average_cost() / multiplier                    
                except ZeroDivisionError, TypeError:
                    # caught error for cases where get_average_cost and quantity may be None
                    unreal_pl = float('nan')
                    percent_gain_loss = float('nan')
                    average_px = float('nan')
                            
            else:
                
                pos_delta = self.get_quantity() * 1.0 * \
                               PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier'] 
                pos_theta = 0
                gamma_percent = 0
                # (S - X) * pos * multiplier
                unreal_pl = (self.instrument.get_tick_value(4) - self.get_average_cost() ) * self.get_quantity() * \
                               PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier']
                               
                sign = abs(self.get_quantity()) / self.get_quantity()                                
                percent_gain_loss = sign * (spot_px - self.get_average_cost() / multiplier) / (self.get_average_cost() / multiplier) * 100
                average_px = self.get_average_cost() / multiplier
                        
            self.set_port_field(PortfolioItem.POSITION_DELTA, pos_delta)
            self.set_port_field(PortfolioItem.POSITION_THETA, pos_theta)
            self.set_port_field(PortfolioItem.GAMMA_PERCENT, gamma_percent)
            self.set_port_field(PortfolioItem.UNREAL_PL, unreal_pl)
            self.set_port_field(PortfolioItem.AVERAGE_PRICE, average_px)
            self.set_port_field(PortfolioItem.PERCENT_GAIN_LOSS, percent_gain_loss)
            
        except Exception, err:
            
            logging.error(traceback.format_exc())     

                        
        #logging.info('PortfolioItem:calculate_pl. %s' % self.dump())
    
    def update_position(self, position, average_cost, extra_info):
        self.set_port_field(PortfolioItem.POSITION, position)
        self.set_port_field(PortfolioItem.AVERAGE_COST, average_cost)
        if extra_info:
            self.set_port_field(PortfolioItem.MARKET_VALUE, extra_info['market_value'])
        
        
        
    def dump(self):
        s= ", ".join('[%s:%8.2f]' % (k, v) for k,v in self.port_fields.iteritems())
        return 'PortfolioItem contents: %s %s %s' % (self.contract_key, self.account_id, s)
    
    
class Portfolio(AbstractTableModel):
    '''
        portfolio : 
             {
                'port_items': {<contract_key>, PortItem}, 
                'opt_chains': {<oc_id>: option_chain}, 
                'g_table':{'rows':{...} , 'cols':{...}, 
                           'header':{...},
                           'row_index': <curr_index>,
                           'ckey_to_row_index':{<contract_key>: <row_id>}, 
                           'row_to_ckey_index':{<row_id>: <contract_key>}
                                            
             }   
                
    '''    
    def __init__(self, account):
        self.account = account
        self.create_empty_portfolio()
        AbstractTableModel.__init__(self)
        
    def is_contract_in_portfolio(self, contract_key):
        return self.get_portfolio_port_item(contract_key)
            
    def get_portfolio_port_item(self, contract_key):
        try:
            return self.port['port_items'][contract_key]
        except KeyError:
            return None
        
    def create_empty_portfolio(self):
        self.port = {}
        self.port['port_items']=  {}
        self.port['opt_chains']=  {}
        
        
        self.port['g_table']=  {'row_index': 0, 'ckey_to_row_index': {}, 'row_to_ckey_index': {}}
    
        self.init_table()
        return self.port        

    
    def set_portfolio_port_item(self, contract_key, port_item):
        self.port['port_items'][contract_key] = port_item
        
        '''
            update the gtable contract_key to row number index
        '''
        self.update_ckey_row_xref(contract_key, port_item)
        
                
    def is_oc_in_portfolio(self, oc_id):
        try:
            return self.port['opt_chains'][oc_id]
        except KeyError:
            return None

    def get_option_chain(self, oc_id):
        return self.is_oc_in_portfolio(oc_id)
        
    def set_option_chain(self, oc_id, oc):
        self.port['opt_chains'][oc_id] = oc

    def get_option_chains(self):
        return self.port['opt_chains']

    def calculate_item_pl(self, contract_key):
        self.port['port_items'][contract_key].calculate_pl(contract_key)
        

    def dump_portfolio(self):
        #<account_id>: {'port_items': {<contract_key>, instrument}, 'opt_chains': {<oc_id>: option_chain}}
        
        def print_port_items(x):
            return '[%s]: %s %s' % (x[0],  ', '.join('%s: %s' % (k,str(v)) for k, v in x[1].get_port_fields().iteritems()),
                                           ', '.join('%s: %s' % (k,str(v)) for k, v in x[1].get_instrument().get_tick_values().iteritems()))
        
        p_items = map(print_port_items, [x for x in self.port['port_items'].iteritems()])
        logging.info('PortfolioMonitor:dump_portfolio %s' % ('\n'.join(p_items)))
        return '\n'.join(p_items)
    
    
    
    
    
    
    '''
        implement AbstractTableModel methods and other routines
    '''
    def init_table(self):
        self.port['g_table']['header'] = [('symbol', 'Symbol', 'string'), ('right', 'Right', 'string'), ('avgcost', 'Avg Cost', 'number'), ('market_value', 'Market Value', 'number'), 
                  ('avgpx', 'Avg Price', 'number'), ('spotpx', 'Spot Price', 'number'), ('pos', 'Quantity', 'number'), 
                  ('delta', 'Delta', 'number'), ('theta', 'Theta', 'number'), ('gamma', 'Gamma', 'number'), 
                  ('pos_delta', 'P. Delta', 'number'), ('pos_theta', 'P. Theta', 'number'), ('gamma_percent', 'P. Gamma', 'number'), 
                  ('unreal_pl', 'Unreal P/L', 'number'), ('percent_gain_loss', '% gain/loss', 'number'),
                  ('symbolid', 'Sym Id', 'string')
                  ]  
    def update_ckey_row_xref(self, contract_key, port_item):
        
#         if port_item.get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'] and \
#            port_item.get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type']:
        row_id = self.port['g_table']['row_index']
        self.port['g_table']['ckey_to_row_index'][contract_key] = row_id
        self.port['g_table']['row_to_ckey_index'][row_id] = contract_key
        self.port['g_table']['row_index'] += 1
  
    def ckey_to_row(self, contract_key):
        return self.port['g_table']['ckey_to_row_index'][contract_key]
  
    def get_column_count(self):
        return len(self.port['g_table']['header'])
    
    def get_row_count(self):
        p_items = [x for x in self.port['port_items'].iteritems()]
#         p1_items = filter(lambda x: x[1].get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'], p_items)
#         p2_items = filter(lambda x: x[1].get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type'], p1_items)
#         return len(p2_items)
        return len(p_items)
    
    
    def get_column_name(self, col):
        return self.port['g_table']['header'][col][1]


    def get_column_id(self, col):
        return self.port['g_table']['header'][col][0]

    def get_value_at(self, row, col):
#         ckey = self.port['g_table']['row_to_ckey_index'][row]
#         p_item = self.port['port_items'][ckey]
        raise NotImplementedError
    
    def get_values_at(self, row):
        ckey = self.port['g_table']['row_to_ckey_index'][row]
        p_item = self.port['port_items'][ckey]
        return self.port_item_to_row_fields((None, p_item))
    
    def port_item_to_row_fields(self, x):
        
        def handle_NaN(n):
            # the function JSON.parse will fail at the javascript side if it encounters
            # a NaN value in the json string. Convert Nan to null to circumvent the issue 
            try:
                return None if math.isnan(n) else n
            except:
                return None 
        
        rf = [{'v': '%s-%s-%s' % (x[1].get_symbol_id(), x[1].get_expiry(), x[1].get_strike())}, 
             {'v': x[1].get_right()},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.AVERAGE_COST))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.MARKET_VALUE))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.AVERAGE_PRICE))},
             {'v': handle_NaN(self.get_spot_px(x[1]))},
             {'v': x[1].get_quantity()},
             {'v': handle_NaN(x[1].get_instrument().get_tick_value(Option.DELTA))},
             {'v': handle_NaN(x[1].get_instrument().get_tick_value(Option.THETA))},
             {'v': handle_NaN(x[1].get_instrument().get_tick_value(Option.GAMMA))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.POSITION_DELTA))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.POSITION_THETA))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.GAMMA_PERCENT))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.UNREAL_PL))},
             {'v': handle_NaN(x[1].get_port_field(PortfolioItem.PERCENT_GAIN_LOSS))},
             {'v': x[1].get_symbol_id()}
             ]
        return rf     
    
    
    def set_value_at(self, row, col, value):
        pass
    

    
    def get_spot_px(self, x):
        px = float('nan')
        if x.get_quantity() > 0:
            px= x.get_instrument().get_tick_value(Symbol.BID)
        elif x.get_quantity() < 0:
            px= x.get_instrument().get_tick_value(Symbol.ASK)
        if px == -1:
            return x.get_instrument().get_tick_value(Symbol.LAST)
    
        return px
    
    def get_JSON(self):
        dtj = {'cols':[], 'rows':[], 'ckey_to_row_index':{}}
        # header fields      
        map(lambda hf: dtj['cols'].append({'id': hf[0], 'label': hf[1], 'type': hf[2]}), self.port['g_table']['header'])
        
        #p_items = sorted([x for x in self.port['port_items'].iteritems()])
        
        # create a list of port items tuples (contract_key, port_item) ordered by row_id
        # that is in the order when each items was created and inserted into the map
        # this ensures that the same sequence is replicated to the google datatable
        p_items = map(lambda x:(self.port['g_table']['row_to_ckey_index'][x], 
                        self.port['port_items'][ self.port['g_table']['row_to_ckey_index'][x] ]), range(self.port['g_table']['row_index']))
        

        #p1_items = filter(lambda x: x[1].get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'], p_items)
        #p2_items = filter(lambda x: x[1].get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type'], p1_items)
        #map(lambda p: dtj['rows'].append({'c': self.port_item_to_row_fields(p)}), p2_items)
        map(lambda p: dtj['rows'].append({'c': self.port_item_to_row_fields(p)}), p_items)
        
        
        return json.dumps(dtj) #, indent=4)     
    
    def dump_table_index_map(self):
        return '\n'.join('[%d]:%s' % (x[0], x[1]) for x in  self.port['g_table']['row_to_ckey_index'].items())       
        