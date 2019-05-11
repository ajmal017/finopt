# -*- coding: utf-8 -*-
import sys, traceback
import json
import logging
from time import sleep
import threading
import time, datetime
from finopt.instrument import Symbol
from misc2.helpers import ContractHelper
from misc2.observer import Subscriber
import os

class StockOptionsSnapshot(threading.Thread, Subscriber):
    
    def __init__(self, config):
        threading.Thread.__init__(self)
        Subscriber.__init__(self, 'StockOptionsSnapshot')
        self.symbols = {}
        self.run_forever()
        self.persist={}
        self.persist['is_persist'] = config["stkopt.is_persist"]
        self.persist['persist_dir'] =config["stkopt.persist_dir"]
        self.persist['file_exist'] = False
        self.persist['spill_over_limit'] = int(config["stkopt.spill_over_limit"])
        
        
    def run_forever(self):
        self.start()
    
    def set_stop(self):
        self.stop = True
        
    
    def event_read_file_line(self, event, record=None):
        print record
    
    def tickPrice(self, event, **params):
        logging.debug('StockOptionsSnapshot:tickPrice')
        try:
            contract_key = params['contract_key']
            s = self.symbols[contract_key]
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(params['field'], params['price'])        
    
    def tickSize(self, event, **params):
        logging.debug('QuoteHandler:ticksize')
        try:
            contract_key = params['contract_key']
            s = self.symbols[contract_key]
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        s.set_tick_value(params['field'], params['size'])


    #def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
    def tickOptionComputation(self, event, **params):
        logging.debug('QuoteHandler:tickOptionComputation')
        try:
            contract_key = params['contract_key']
            s = self.symbols[contract_key]
        except KeyError:
            s = Symbol(ContractHelper.makeContractfromRedisKeyEx(contract_key))
            self.symbols[contract_key] = s
        greeks = dict(params)
        del greeks['field']
        del greeks['contract_key']
        s.set_ib_option_greeks(params['field'], greeks)       
    
    def save_record(self, ts, symbol_key):
        if self.persist['file_exist'] == False:
            if not os.path.isdir(self.persist['persist_dir']):
                os.mkdir(self.persist['persist_dir'])
            fn = '%s/ibkdump-%s.txt' % (self.persist['persist_dir'], datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            logging.debug('write_message_to_file: path %s', fn)
            self.persist['fp'] = open(fn, 'w')
            self.persist['file_exist'] = True
            self.persist['spill_over_count'] = 0
            self.persist['fp'].write('#\r\n')
            self.persist['fp'].write('# [time][symbol]0----1----2----3-----4----5----6----7----8----9----10----11----12----13----14----15-----[bid greeks: impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice][ask greeks: impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice][last greeks: impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice]\r\n')
            self.persist['fp'].write('#               bsiz last ask  asiz  lsiz high low  volm cls                                     opentic\r\n')
            self.persist['fp'].write('#\r\n')
        
        def create_record_str(symbol_key):
            tickvals = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            for item in self.symbols[symbol_key].get_tick_values().items():
                tickvals[item[0]] = item[1]
                
            opt_fields = [(Symbol.BID_OPTION, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]), 
                          (Symbol.ASK_OPTION, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]), 
                          (Symbol.LAST_OPTION, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])]
            for ofld in opt_fields:
                try:
                    gdict = self.symbols[symbol_key].get_ib_option_greeks(ofld[0])
                    ofld[1][0] = gdict['impliedVol']
                    ofld[1][1] = gdict['delta']
                    ofld[1][2] = gdict['optPrice']
                    ofld[1][3] = gdict['pvDividend']
                    ofld[1][4] = gdict['gamma']
                    ofld[1][5] = gdict['vega']
                    ofld[1][6] = gdict['theta']
                    ofld[1][7] = gdict['undPrice'] 
                except:
                    continue
            ofld_str = ''
            for greek_blob in opt_fields:
                ofld_str += ''.join('%0.4f,' % e for e in greek_blob[1])
            return '%s,%s,%s,%s\r' % (ts, symbol_key,  ','.join('%0.2f' % e for e in tickvals), ofld_str)
            
            
        s = create_record_str(symbol_key)
        self.persist['fp'].write(s)
        self.persist['fp'].flush()
        print s
        self.persist['spill_over_count'] +=1
        #print self.persist['spill_over_count']
        if self.persist['spill_over_count'] == self.persist['spill_over_limit']:
            self.persist['fp'].flush()
            self.persist['fp'].close()
            self.persist['file_exist'] = False
    
        
    
    def run(self):
        self.stop = False
        self.record_to_file = True

        def save_all_records():
            for k in self.symbols.keys():
                self.save_record(now.strftime("%H:%M:%S"), k)

        save_all_records()
        while not self.stop:
            now =  datetime.datetime.now()
            zz = now.strftime("%S")
            if zz == '00' and self.record_to_file:
                self.record_to_file = False
                save_all_records()
                logging.info( 'write stuff to file at one minute interval %s' % now.strftime("%H:%M:%S"))
            elif zz <> '00':
                self.record_to_file = True
            
    
            sleep(0.1)