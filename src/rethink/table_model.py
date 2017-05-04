from misc2.observer import Subscriber, Publisher
from misc2.observer import NotImplementedException
import logging

class AbstractTableModel(Publisher):
    
    EVENT_TM_TABLE_CELL_UPDATED = 'event_tm_table_cell_updated'
    EVENT_TM_TABLE_ROW_INSERTED = 'event_tm_table_row_inserted'
    EVENT_TM_TABLE_ROW_UPDATED = 'event_tm_table_row_updated'
    TM_EVENTS = [EVENT_TM_TABLE_CELL_UPDATED, EVENT_TM_TABLE_ROW_INSERTED, EVENT_TM_TABLE_ROW_UPDATED]
    
    def __init__(self):
        Publisher.__init__(self, AbstractTableModel.TM_EVENTS)
    
    def register_listener(self, listener):
        try:
            map(lambda e: self.register(e, listener, getattr(listener, e)), AbstractTableModel.TM_EVENTS)
        except AttributeError as e:
            logging.error("AbstractTableModel:register_listener. Function not implemented in the listener. %s" % e)
            raise NotImplementedException        
    
    def fire_table_row_updated(self, row):
        self.dispatch(AbstractTableModel.EVENT_TM_TABLE_ROW_UPDATED, {'row': row})
    
    def fire_table_row_inserted(self, row):
        self.dispatch(AbstractTableModel.EVENT_TM_TABLE_ROW_INSERTED, {'row': row})
        
        
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
    
    def set_value_at(self, row, col, value):
        raise NotImplementedException
    
    def insert_row(self, values):
        raise NotImplementedException

    
