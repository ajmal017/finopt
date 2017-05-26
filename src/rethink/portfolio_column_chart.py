# -*- coding: utf-8 -*-
import sys, traceback, logging
from rethink.portfolio_item import PortfolioRules, PortfolioItem, Portfolio
import numpy as np
from rethink.table_model import AbstractPortfolioTableModelListener, AbstractTableModel
from ib.ext.Contract import Contract
    
class PortfolioColumnChart():

    '''
    
        code to handle portfolio column chart
    
    
        [[ 0.   0.   0.  -2.   0. ]
         [ 0.   0.   0.  -1.   0. ]
         [ 0.   0.   0.   0.   0. ]
         [ 0.   0.   0.  -3.   0. ]
         [ 0.   0.   0.   1.   0. ]
         [ 0.   0.   0.  -4.   0. ]
         [ 0.   0.   0.   1.   0. ]
         [ 0.  -2.   0.   1.   0. ]
         [ 0.  -2.   1.   0.   0. ]
         [ 0.   0.   0.   0.   2.6]
         [-3.   0.   0.   0.   0. ]
         [ 0.   0.   0.   0.   0. ]
         [-1.   0.   0.   0.   0. ]
         [ 0.   0.  -3.   0.   0. ]
         [ 0.   0.  -1.   0.   0. ]]

        
        
    '''
    def __init__(self, pf):
        self.pf = pf
        self.xy_arr = None
        self.col_header = {'y_map': None, 'y_map_reverse': None}
        self.row_header = {'x_map': None, 'x_map_reverse': None}
    
    def get_object_name(self):
        return 'p-%s' % (id(self))   
    
    def ckey_to_row(self, contract_key):
        
        p_item= self.pf.is_contract_in_portfolio(contract_key)
        if p_item:
            if p_item.get_instrument_type() == 'OPT':
                row = self.row_header['x_map'][p_item.get_strike()]
                return row
            else:  #FUT average price 
                col = self.col_header['y_map'][self.make_col_header(p_item)]
                for i in self.xy_arr.shape[0]:
                    if self.xy_arr[i][col] <> 0:
                        return i
        
           
    def get_values_at(self, row):
        rf = [{'v': self.row_header['x_map_reverse'][row]}]
        for rv in self.xy_arr[row]:
            rf.append({'v': rv})
        
        return rf
           
    def make_col_header(self, p_item):
        return  '%s-%s-%s-%s' % (p_item.get_expiry(),  p_item.get_symbol_id(), 
                                                    p_item.get_instrument_type(), p_item.get_right() )  
    
    def get_JSON(self):  
        
        
        p2_items = self.pf.get_portfolio_port_items().values()
        p1_items = filter(lambda x: x.get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'], p2_items)
        p_items = filter(lambda x: x.get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type'], p1_items)
        
        print ','.join(str(x.get_strike()) for x in p_items)
        # row values domain
        # find out the strikes of all contracts purchased, for futures, use the average cost 
        x_range  = set(
                    map(lambda x:int(round(x.get_strike(),-1)), filter(lambda x: x.get_instrument_type() in 'OPT', p_items)) +   
                    map(lambda x:int(round(x.get_port_field(PortfolioItem.AVERAGE_PRICE),-1)), filter(lambda x: x.get_instrument_type() in 'FUT', p_items))
                   )
    
        # column values domain (month,symbol,contract_type, right)
        y_range = set(map(self.make_col_header, p_items))
        


        # sort the list then create a map that stores kv pairs of value:index_pos
        x_range = sorted(list(x_range))
        print x_range
        x_map = {}
        x_map_reverse = {}
        for i in range(len(x_range)):
            x_map[x_range[i]] = i
            x_map_reverse[i] = x_range[i]
        print '---xmap and xmap reverse' 
        print x_map
        print x_map_reverse
        print '----'
        self.row_header['x_map'] = x_map
        self.row_header['x_map_reverse'] = x_map_reverse

        y_range = sorted(list(y_range))
        y_map = {}
        y_map_reverse = {}
        for i in range(len(y_range)):
            y_map[y_range[i]] = i
            y_map_reverse[i]= y_range[i]  
        
        print '---ymap and ymap reverse' 
        print y_map
        print y_map_reverse
        print '----'
        self.col_header['y_map'] = y_map
        self.col_header['y_map_reverse'] = y_map_reverse
        
        def set_ij(p_item):
            if p_item.get_instrument_type() == 'FUT':
                i = x_map[int(round(p_item.get_port_field(PortfolioItem.AVERAGE_PRICE), -1))]
                #
                # hard code logic below to convert number of MHI in HSI unit (that is 5:1)
                v = p_item.get_quantity() *\
                 (PortfolioRules.rule_map['option_structure'][p_item.get_symbol_id()]['multiplier'] /
                  PortfolioRules.rule_map['option_structure']['HSI']['multiplier'] 
                  )
                
                
            else:
                i = x_map[int(round(p_item.get_strike(), -1))]
                v = p_item.get_quantity()
            j = y_map[self.make_col_header(p_item)]
            
            return (i,j,v)
        
        ijv_dist = map(set_ij, p_items)
        print ijv_dist
        xy_arr = np.zeros((len(x_range), len(y_range)))
        
        
        
        def update_ijv(ijv):
            xy_arr[ijv[0], ijv[1]] = ijv[2]
            return 1
        
        map(update_ijv, ijv_dist)
        print xy_arr
        
        def gen_datatable():
            ccj = {'cols':[{'id': 'strike', 'label': 'strike', 'type': 'number'}], 'rows':[]}
            for j in range(len(y_map_reverse)):
                ccj['cols'].append({'id': y_map_reverse[j], 'label': y_map_reverse[j], 'type': 'number' })
                
            def row_item(i):
                for m in range(xy_arr.shape[1]):
                    ccj['rows'][i]['c'].append({'v':xy_arr[i][m]})     
                               
            for i in range(len(x_map_reverse)):
                ccj['rows'].append({'c':[{'v':x_map_reverse[i]}]})
                row_item(i)
            return ccj    
                
        return gen_datatable()
    
    
class PortfolioColumnChartTM(PortfolioColumnChart, AbstractTableModel, AbstractPortfolioTableModelListener):
    
    
    def __init__(self, name, pf,  kproducer):
        
        PortfolioColumnChart.__init__(self, pf)
        AbstractTableModel.__init__(self)
        AbstractPortfolioTableModelListener.__init__(self, name)
        self.request_ids = {}
        self.kproducer = kproducer
        
        
        
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
        logging.info("[PortfolioColumnChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
    
    def event_tm_request_table_structure(self, event, request_id, target_resource, account):
        self.request_ids[request_id] = {'request_id': request_id, 'account': account}
        logging.info("[PortfolioColumnChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
               
    