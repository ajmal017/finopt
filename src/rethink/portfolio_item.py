# -*- coding: utf-8 -*-
import sys, traceback
import logging
import json
import time, datetime
import copy
from optparse import OptionParser
from time import sleep
from misc2.helpers import ContractHelper
from finopt.instrument import Symbol, Option
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from numpy import average

class PortfolioRules():
    rule_map = {
                'symbol': {'HSI' : 'FUT', 'MHI' : 'FUT', 'QQQ' : 'STK'},
                'expiry': {'HSI' : 'same_month', 'MHI': 'same_month', 'STK': 'leave_blank'},
                'option_structure': {
                                        'HSI': {'spd_size': 200, 'multiplier': 50, 'rate': 0.0012, 'div': 0, 'trade_vol':0.15},
                                        'MHI': {'spd_size': 200, 'multiplier': 10, 'rate': 0.0012, 'div': 0, 'trade_vol':0.15}
                                        
                                    },
                'exchange': {'HSI': 'HKFE', 'MHI': 'HKFE'},
                
                
               } 
    
class PortfolioItem():
    """
    
    """
    POSITION = 7001
    AVERAGE_COST = 7002
    POSITION_DELTA = 7003
    POSITION_THETA = 7004
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
    
    def get_symbol_id(self):
        return self.instrument.get_contract().m_symbol
    
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
            if self.get_instrument_type() == 'OPT':
                spot_px = self.instrument.get_tick_value(4)
                multiplier =  PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier']
                
                pos_delta = self.get_quantity() * self.instrument.get_tick_value(Option.DELTA) * multiplier                                
                pos_theta = self.get_quantity() * self.instrument.get_tick_value(Option.THETA) * multiplier
                               

                #(spot premium * multiplier - avgcost) * pos)
                unreal_pl = (spot_px * multiplier - self.get_average_cost()) * self.get_quantity()
                #print "%f %f %d" % (spot_px, self.get_average_cost(), multiplier)
                percent_gain_loss = (1 - spot_px / (self.get_average_cost() / multiplier)) * 100 \
                                        if self.get_quantity() < 0 else \
                                        (spot_px - self.get_average_cost() / multiplier) / (self.get_average_cost() / multiplier) * 100 
                                    
                                     
                            
                            
            else:
                pos_delta = self.get_quantity() * 1.0 * \
                               PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier'] 
                pos_theta = 0
                # (S - X) * pos * multiplier
                unreal_pl = (self.instrument.get_tick_value(4) - self.get_average_cost() ) * self.get_quantity() * \
                               PortfolioRules.rule_map['option_structure'][self.get_symbol_id()]['multiplier']
                               
                sign = abs(self.get_quantity()) / self.get_quantity()                                
                percent_gain_loss = sign * (spot_px - self.get_average_cost() / multiplier) / (self.get_average_cost() / multiplier) * 100
                        
            self.set_port_field(PortfolioItem.POSITION_DELTA, pos_delta)
            self.set_port_field(PortfolioItem.POSITION_THETA, pos_theta)
            self.set_port_field(PortfolioItem.UNREAL_PL, unreal_pl)
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
