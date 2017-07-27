# -*- coding: utf-8 -*-
import sys, traceback, logging, json
from rethink.portfolio_item import PortfolioRules, PortfolioItem, Portfolio
import numpy as np
from rethink.table_model import AbstractPortfolioTableModelListener, AbstractTableModel
from ib.ext.Contract import Contract
    
class PortfolioColumnChart():

    '''
    
        code to handle portfolio column chart
        
        vertical down: each row (i) is a strike price for an option or the avg px for a futures contract
        across: each column (j) is the contract type
        the value [i,j] stores the quantity of a contract (j) with the corresponding strike/avg cost (i)
    
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
        self.col_header = {'j_contract_to_col_num': None, 'j_contract_to_col_num_reverse': None}
        self.row_header = {'i_strike_to_row_num': None, 'i_strike_to_row_num_reverse': None}
        self.last_tally = 0
        self.never_been_run = True
    
    def get_object_name(self):
        #return json.dumps({'account': self.pf.account, 'id': id(self), 'class': self.__class__.__name__})
        return {'account': self.pf.account, 'id': id(self), 'class': self.__class__.__name__}
    
    def ckey_to_row(self, contract_key):
        
        p_item= self.pf.is_contract_in_portfolio(contract_key)
        if p_item:
            if p_item.get_instrument_type() == 'OPT':
                logging.info('*********************8 ckey_to_row: %d' % int(p_item.get_strike()))
                row = self.row_header['i_strike_to_row_num'][int(p_item.get_strike())]
                logging.info('*********************8 ckey_to_row row # %d' % row)
                return row
            else:  #FUT average price 
                col = self.col_header['j_contract_to_col_num'][self.make_col_header(p_item)]
                for i in self.xy_arr.shape[0]:
                    if self.xy_arr[i][col] <> 0:
                        return i
    
    def update_tally_count(self):
        self.last_tally = self.count_tally()
    
    
    def get_xy_array(self):
        return self.xy_arr
    
    
    def get_last_tally(self):
        return self.last_tally   
           
    def get_values_at(self, row):
        rf = [{'v': self.row_header['i_strike_to_row_num_reverse'][row]}]
        for rv in self.xy_arr[row]:
            rf.append({'v': rv})
        
        return rf
           
    def make_col_header(self, p_item):
        return  '%s-%s-%s-%s' % (p_item.get_expiry(),  p_item.get_symbol_id(), 
                                                    p_item.get_instrument_type(), p_item.get_right() )  
    
    
    def count_tally(self):
        p2_items = self.pf.get_portfolio_port_items().values()
        p1_items = filter(lambda x: x.get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'], p2_items)
        p_items = filter(lambda x: x.get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type'], p1_items)
        try:
            tally =  reduce(lambda x,y: x+y, map(lambda x: abs(x.get_quantity()), p_items))
            return tally
        except:
            return 0
    
    def get_JSON(self):  
        
        
        p2_items = self.pf.get_portfolio_port_items().values()
        p1_items = filter(lambda x: x.get_symbol_id() in PortfolioRules.rule_map['interested_position_types']['symbol'], p2_items)
        
# 2017-7-26
# filter out zero positions
        p0_items = filter(lambda x: x.get_port_field(PortfolioItem.POSITION) <> 0, p1_items)
        p_items = filter(lambda x: x.get_instrument_type() in  PortfolioRules.rule_map['interested_position_types']['instrument_type'], p0_items)
        
        
        #i_strikes_range ','.join(str(x.get_strike()) for x in p_items)
        # row values domain
        # find out the strikes of all contracts purchased, for futures, use the average cost 
        
#         for p in p_items:
#             print '%f %s' % (p.get_port_field(PortfolioItem.AVERAGE_COST), p.get_instrument_type() )
            
        
        i_strikes_range  = set(
                    map(lambda x:int(round(x.get_strike(),-1)), filter(lambda x: x.get_instrument_type() in 'OPT', p_items)) +   
                    map(lambda x:int(round(x.get_port_field(PortfolioItem.AVERAGE_COST) / 10,-1)), filter(lambda x: x.get_instrument_type() in 'FUT', p_items))
                   )
    
        # column values domain (month,symbol,contract_type, right)
        j_contract_types = set(map(self.make_col_header, p_items))
        


        # sort the list then create a map that stores kv pairs of value:index_pos
        i_strikes_range = sorted(list(i_strikes_range))
        #print i_strikes_range
        i_strike_to_row_num = {}
        i_strike_to_row_num_reverse = {}
        for m in range(len(i_strikes_range)):
            i_strike_to_row_num[i_strikes_range[m]] = m
            i_strike_to_row_num_reverse[m] = i_strikes_range[m]
        
        #print '---xmap and xmap reverse' 
        #print i_strike_to_row_num
        #print i_strike_to_row_num_reverse
        #print '---'
        
        
        self.row_header['i_strike_to_row_num'] = i_strike_to_row_num
        self.row_header['i_strike_to_row_num_reverse'] = i_strike_to_row_num_reverse

        j_contract_types = sorted(list(j_contract_types))
        j_contract_to_col_num = {}
        j_contract_to_col_num_reverse = {}
        for n in range(len(j_contract_types)):
            j_contract_to_col_num[j_contract_types[n]] = n
            j_contract_to_col_num_reverse[n]= j_contract_types[n]  
        
#         print '---ymap and ymap reverse' 
        #print j_contract_to_col_num
#         print j_contract_to_col_num_reverse
#         print '----'

        self.col_header['j_contract_to_col_num'] = j_contract_to_col_num
        self.col_header['j_contract_to_col_num_reverse'] = j_contract_to_col_num_reverse
        
        def set_ij(p_item):
            if p_item.get_instrument_type() == 'FUT':
                i = i_strike_to_row_num[int(round(p_item.get_port_field(PortfolioItem.AVERAGE_PRICE), -1))]
                #
                # hard code logic below to convert number of MHI in HSI unit (that is 5:1)
                v = p_item.get_quantity() *\
                 (PortfolioRules.rule_map['option_structure'][p_item.get_symbol_id()]['multiplier'] /
                  PortfolioRules.rule_map['option_structure']['HSI']['multiplier'] 
                  )
                
                
            else:
                i = i_strike_to_row_num[int(round(p_item.get_strike(), -1))]
                v = p_item.get_quantity()
            j = j_contract_to_col_num[self.make_col_header(p_item)]
            
            return (i,j,v)
        
        ijv_dist = map(set_ij, p_items)
        
        xy_arr = np.zeros((len(i_strikes_range), len(j_contract_types)))
        
        
        
        def update_ijv(ijv):
            xy_arr[ijv[0], ijv[1]] = ijv[2]
            return 1
        
        map(update_ijv, ijv_dist)
        print xy_arr
        logging.info('PortfolioColumnChart: JSON array->\n%s' % xy_arr)
        
        def gen_datatable():
            ccj = {'cols':[{'id': 'strike', 'label': 'strike', 'type': 'number'}], 'rows':[]}
            for j in range(len(j_contract_to_col_num_reverse)):
                ccj['cols'].append({'id': j_contract_to_col_num_reverse[j], 'label': j_contract_to_col_num_reverse[j], 'type': 'number' })
                
            def row_item(i):
                for m in range(xy_arr.shape[1]):
                    ccj['rows'][i]['c'].append({'v':xy_arr[i][m]})     
                               
            for i in range(len(i_strike_to_row_num_reverse)):
                ccj['rows'].append({'c':[{'v':i_strike_to_row_num_reverse[i]}]})
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
        logging.info("[PortfolioColumnChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
        
        
    
    def event_tm_request_table_structure(self, event, request_id, target_resource, account):
        self.request_ids[request_id] = {'request_id': request_id, 'account': account}
        logging.info("[PortfolioColumnChartTM:%s] received %s content:[%s]" % (self.name, event, vars()))
        
        logging.info("[PortfolioColumnChartTM: self class: %s, incoming target class %s" % (self.get_object_name()['class'], target_resource['class']))
        if target_resource['class'] == self.get_object_name()['class']:
            self.get_kproducer().send_message(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED,                                               
                                      json.dumps({'source': self.get_object_name(), 
                                                  'origin_request_id': request_id, 'account': account, 
                                                  'data_table_json': self.get_JSON()}))
                
               
    