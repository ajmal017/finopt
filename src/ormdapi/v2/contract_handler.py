import logging
import json
from threading import RLock
from misc2.observer import Subscriber
from misc2.observer import NotImplementedException
from misc2.helpers import ContractHelper
from copy import deepcopy
import traceback
from finopt.instrument import Symbol

class ContractHandler(Subscriber):

    
    def __init__(self, name, gw_parent):
        
        self.contract_details = {}
        self.contract_end = {}
        self.name = name
        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        self.hist_data = {}
        self.hist_data_end = {}
        
        
        Subscriber.__init__(self, self.name)
        
        '''
             ask tws_event_handler to forward contract details message to
             this class
             
        '''
        for e in ['contractDetails', 'historicalData', 'error']:
            self.tws_event_handler.register(e, self)           
        


    def handle_contract_details(self, req_id, contract_info, end_batch):
        logging.debug('ContractHandler:handle_contract_details')
        try:
            _ = self.contract_end[req_id]
        except KeyError:
            self.contract_end[req_id] = end_batch
        
        if not end_batch:
            self.contract_end[req_id] = False    
            try:
                cd = self.contract_details[req_id]
            except KeyError:
                cd = self.contract_details[req_id] = {'contract_info': []}
            cd['contract_info'].append(contract_info)
        else:
            self.contract_end[req_id] = True
        
    def handle_historical_data(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        logging.info('ContractHandler:handle_historical_data %s %0.2f' % (date, close))
        
        try:
            _ = self.hist_data_end[reqId]
        except KeyError:
            self.hist_data_end[reqId] = False
            
        self.hist_data_end[reqId] = True if 'finished-' in date else False
        
        if not self.hist_data_end[reqId]:
            try:
                _ = self.hist_data[reqId]
            except KeyError:
                self.hist_data[reqId] = [['date', 'open', 'high', 'low', 'close', 'volume', 'count', 'WAP', 'hasGaps']]
            
#            self.hist_data[reqId].append({'date': date, 'open': open, 'high': high, 'low': low, 
#                                          'close': close, 'volume': volume, 'count': count, 'WAP': WAP, 'hasGaps': hasGaps})
            self.hist_data[reqId].append([date, open, high, low, close, volume, count, WAP, hasGaps])
        else:
            logging.info('ContractHandler:handle_historical_data COMPLETE DOWNLOAD request id [%d]!' % reqId)  
        
    def update(self, event, **param): 
        if event == 'contractDetails':
            self.handle_contract_details(**param)
        elif event == 'historicalData':
            self.handle_historical_data(**param)
        elif event == 'error':
            try:
                
                if param['id'] > 3000 and param['id'] < 3999:
                    self.contract_details[param['id']] = {'error': param['errorMsg']}
                    self.contract_end[param['id']] = True
                    
                elif param['id'] > 4000 and param['id'] < 4999:
                    self.hist_data[param['id']] = {'error': param['errorMsg']}
                    self.hist_data_end[param['id']] = True
                    
            except:
                pass
    
    def get_contract_details(self, req_id):
        try:
            _ = self.contract_end[req_id]
            if self.contract_end[req_id] == False:
                return None
            return self.contract_details.pop(req_id, None)    
        except:
            return None
        
        
    def get_historical_data(self, req_id):
        try:
            _ = self.hist_data_end[req_id]
            if self.hist_data_end[req_id] == False:
                return None
            return self.hist_data.pop(req_id, None)    
        except:
            return None        
        
        
    
