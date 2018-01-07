# -*- coding: utf-8 -*-
import sys, traceback, logging, json
from rethink.portfolio_item import PortfolioRules, PortfolioItem, Portfolio
import numpy as np
from rethink.table_model import AbstractTableModelListener, AbstractTableModel
from rethink.option_chain import OptionsChain
    
    
class OptionChainChartTM(OptionsChain, AbstractTableModel, AbstractTableModelListener):
    
    
    def __init__(self, name, pf,  kproducer):
        
        OptionsChain.__init__(self, pf)
        AbstractTableModel.__init__(self)
        AbstractTableModelListener.__init__(self, name)
        self.request_ids = {}
        self.kproducer = kproducer
        
        
    def get_kproducer(self):
        return self.kproducer
      
#     def get_column_count(self):
#         raise NotImplementedException
#     
#     def get_row_count(self):
#         raise NotImplementedException
# 
#     def get_column_name(self, col):
#         raise NotImplementedException
# 
#     def get_column_id(self, col):
#         raise NotImplementedException
# 
#     def get_value_at(self, row, col):
#         raise NotImplementedException
#     
#     def get_values_at(self, row):
#         raise NotImplementedException
#     
#     def set_value_at(self, row, col, value):
#         raise NotImplementedException
#     
#     def insert_row(self, values):
#         raise NotImplementedException


    def event_tm_table_structure_changed(self, event, source, origin_request_id, account, data_table_json):
        logging.info("[OptionChainChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
        
        
    
    def event_tm_request_table_structure(self, event, request_id, target_resource, account):
        self.request_ids[request_id] = {'request_id': request_id, 'account': account}
        logging.info("[OptionChainChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
        
        logging.info("[OptionChainChartTM: self class: %s, incoming target class %s" % (self.get_object_name()['class'], target_resource['class']))
        if target_resource['class'] == self.get_object_name()['class']:
            self.get_kproducer().send_message(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED,                                               
                                      json.dumps({'source': self.get_object_name(), 
                                                  'origin_request_id': request_id, 'account': account, 
                                                  'data_table_json': self.get_JSON()}))
                
               
    