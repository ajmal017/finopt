from misc2.observer import Subscriber, Publisher
from comms.ibgw.base_messaging import BaseMessageListener
from misc2.observer import NotImplementedException
import logging


class AbstractTableModel(Publisher):
    
    EVENT_TM_TABLE_CELL_UPDATED = 'event_tm_table_cell_updated'
    EVENT_TM_TABLE_ROW_INSERTED = 'event_tm_table_row_inserted'
    EVENT_TM_TABLE_ROW_UPDATED = 'event_tm_table_row_updated'
    EVENT_TM_TABLE_STRUCTURE_CHANGED = 'event_tm_table_structure_changed'
    TM_EVENTS = [EVENT_TM_TABLE_CELL_UPDATED, EVENT_TM_TABLE_ROW_INSERTED, EVENT_TM_TABLE_ROW_UPDATED, 
                 EVENT_TM_TABLE_STRUCTURE_CHANGED]
    
    EVENT_TM_REQUEST_TABLE_STRUCTURE = "event_tm_request_table_structure"
    TM_REQUESTS = [EVENT_TM_REQUEST_TABLE_STRUCTURE]
    
    def __init__(self):
        Publisher.__init__(self, AbstractTableModel.TM_EVENTS)
    
    def register_listener(self, listener):
        try:
            map(lambda e: self.register(e, listener, getattr(listener, e)), AbstractTableModel.TM_EVENTS)
        except AttributeError as e:
            logging.error("AbstractTableModel:register_listener. Function not implemented in the listener. %s" % e)
            raise NotImplementedException        
    
    def fire_table_row_updated(self, row, row_values):
        self.dispatch(AbstractTableModel.EVENT_TM_TABLE_ROW_UPDATED, {'row': row, 'row_values': row_values})
    
    def fire_table_row_inserted(self, row, row_values):
        self.dispatch(AbstractTableModel.EVENT_TM_TABLE_ROW_INSERTED, {'row': row, 'row_values': row_values})
     
    def fire_table_structure_changed(self, event, source, origin_request_id, account, data_table_json):
        self.dispatch(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED, {'source': source, 
                                                      'origin_request_id': origin_request_id, 'account': account, 
                                                      'data_table_json': data_table_json})
    def get_column_count(self):
        raise NotImplementedException
    
    def get_row_count(self):
        raise NotImplementedException

    def get_column_name(self, col):
        raise NotImplementedException

    def get_column_id(self, col):
        raise NotImplementedException

    def get_value_at(self, row, col):
        raise NotImplementedException
    
    def get_values_at(self, row):
        raise NotImplementedException
    
    def set_value_at(self, row, col, value):
        raise NotImplementedException
    
    def insert_row(self, values):
        raise NotImplementedException


class AbstractTableModelListener(BaseMessageListener):
    def __init__(self, name):
        BaseMessageListener.__init__(self, name)
        

    def event_tm_table_cell_updated(self, event, source, row, row_values):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))

        
    def event_tm_table_row_inserted(self, event, source, row, row_values):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))

    def event_tm_table_row_updated(self, event, source, row, row_values):   
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))
    
    def event_tm_table_structure_changed(self, event, source, origin_request_id, account, data_table_json, strikes):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))
        
    def event_tm_request_table_structure(self, event, request_id, target_resource, account):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))        

class AbstractPortfolioTableModelListener(AbstractTableModelListener):
    pass




    



