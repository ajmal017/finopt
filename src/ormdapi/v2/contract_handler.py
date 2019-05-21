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
        self.name = name
        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        self.end = False
        
        
        Subscriber.__init__(self, self.name)
        
        '''
             ask tws_event_handler to forward contract details message to
             this class
             
        '''
        for e in ['contractDetails']:
            self.tws_event_handler.register(e, self)           
        

    def handle_contract_details(self, req_id, contract_info, end_batch):
        logging.debug('ContractHandler:handle_contract_details')
        if not end_batch:
            self.end = False
            try:
                cd = self.contract_details[req_id]
            except KeyError:
                cd = self.contract_details[req_id] = {'contract_info': []}
            cd['contract_info'].append(contract_info)
        else:
            self.end = True
        
    
        
    def update(self, event, **param): 
        if event == 'contractDetails':
            self.handle_contract_details(**param)
        
    
    def get_contract_details(self, req_id):
        return self.contract_details.pop(req_id, None)
        
        

