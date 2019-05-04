#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys, traceback
import json
from time import sleep
from misc2.helpers import ContractHelper
from ib.ext.Contract import Contract
from comms.ibgw.base_messaging import BaseMessageListener
from comms.ibgw.tws_event_handler import TWS_event_handler
from Queue import Queue
import threading
import uuid
import numpy as np
from finopt.test_pattern import Subscriber


class OrderManagerException(Exception):
    pass


class OrderIdManager(threading.Thread):

    def __init__(self, tws_conn):
        threading.Thread.__init__(self)        
        self.id_request_q = Queue()
        self.next_valid_q = Queue()
        self.tws_conn = tws_conn
        self.stop = False
        self.tws_pending = False
        self.clordid_id = {}
        
    
    '''
        expects a client order id of md5
    '''        
    def request_id(self, client_id, clordid):
        
        self.id_request_q.put((client_id, clordid))
        self.id_request_q.task_done()
        
    ''' this function is called by tws_event_handler upon
        receiving nextValidId reply from TWS
    '''
    def update_next_valid_id(self, orderId):
        self.next_valid_q.put(orderId)
        self.next_valid_q.task_done()
        self.tws_pending = False
        
    def set_stop(self):
        self.stop = True
        logging.info('OrderIdManager: set stop flag to true')
            
    def run(self):
        while not self.stop:
                            
            if not self.id_request_q.empty():
                if not self.next_valid_q.empty():
                    
                    next_valid_id = self.next_valid_q.get()
                    print 'get next valid %d next_valid_q size->%d' % (next_valid_id, self.next_valid_q.qsize())
                    client_id, clordid = self.id_request_q.get()
                    #
                    # report the assigned id back to the client side
                    self.dispatch_event('OrderRequestAck', {'client_id': client_id, 'clordid': clordid, 'next_valid_id': next_valid_id})
                    self.clordid_id[clordid] = {'client_id': client_id, 'next_valid_id': next_valid_id}

                else:
                    #
                    # request a next id from TWS
                    if not self.tws_pending:
                        self.tws_pending = True
                        logging.info('OrderIdManager: before call to tws_conn.reqIds for the next avail id')
                        self.tws_conn.reqIds(-1)
                        
                
            # to prevent excessive CPU use
            sleep(0.1)
            
    
    def assigned_id(self, clordid):
        try:
            return self.clordid_id[clordid]
        except KeyError:
            return None
        
    def dispatch_event(self, event, dict):
        logging.info ('OrderIdManager: [%s] => %s' % (event, json.dumps(dict)))



class OrderIdManagerFake(threading.Thread):

    def __init__(self, tws_conn):
        threading.Thread.__init__(self)        
        self.id_request_q = Queue()
        self.next_valid_q = Queue()
        self.tws_conn = tws_conn
        self.stop = False
        self.tws_pending = False
        
        #wait_th = threading.Thread(target=get_user_input, args=(response,))
        self.fake_id = 1
    
    '''
        expects a client order id of md5
    '''        
    def on_id_request(self, client_id, clordid):
        
        self.id_request_q.put((client_id, clordid))
        self.id_request_q.task_done()
        

        
    ''' TWS events
    '''
    def nextValidId(self, orderId):
        self.next_valid_q.put(orderId)
        self.next_valid_q.task_done()
        self.tws_pending = False
        
            
    def run(self):
        while not self.stop:
                            
            if not self.id_request_q.empty():
                print 'id_request_q size %d' % self.id_request_q.qsize()
                if not self.next_valid_q.empty():
                    
                    next_valid_id = self.next_valid_q.get()
                    print 'get next valid %d next_valid_q size->%d' % (next_valid_id, self.next_valid_q.qsize())
                    client_id, clordid = self.id_request_q.get()
                    #
                    # dispatch ?
                    self.dispatch_event('OrderRequestAck', {'client_id': client_id, 'clordid': clordid, 'next_valid_id': next_valid_id})

                else:
                    #
                    # request a next id from TWS
                    if not self.tws_pending:
                        self.tws_pending = True
                        #self.tws_conn.reqIds(-1)
                        self.fake_gen_id()
                
            # to prevent excessive CPU use
            sleep(0.1)
            
            
    def fake_gen_id(self):
        print 'fake id %d' % self.fake_id
        sleep(0.4)
        self.nextValidId(self.fake_id)
        self.fake_id+=1
        
    def dispatch_event(self, event, dict):
        #logging.info('OrderIdManager: [%s] => %s', (event, json.dumps(dict)))
        print 'OrderIdManager: [%s] => %s' % (event, json.dumps(dict))
    

class OrderBook(Subscriber):
    
    OPEN_ORDER_STATUS = ['PendingSubmit', 'PendingCancel','PreSubmitted','Submitted',
                                   'Inactive']
    
    def __init__(self, name):
        '''
            orderbook:
 
            
                
        '''
        Subscriber.__init__(self, name)
        self.name = name
        self.open_order = False 
        self.orders = {}
        
    def handle_order_status(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        try:
            _ = self.orders[orderId]
        except KeyError:
            self.orders[orderId]= {'ord_status':{}, 'error':{}, 'order':{}, 'contract': {}, 'state':{}}
             
        self.orders[orderId]['ord_status']['status'] = status
        self.orders[orderId]['ord_status']['filled'] = filled
        self.orders[orderId]['ord_status']['remaining'] = remaining
        self.orders[orderId]['ord_status']['avgFillPrice'] = avgFillPrice
        self.orders[orderId]['ord_status']['permId'] = permId
        self.orders[orderId]['ord_status']['parentId'] = parentId
        self.orders[orderId]['ord_status']['lastFillPrice'] = lastFillPrice
        self.orders[orderId]['ord_status']['clientId'] = clientId
        self.orders[orderId]['ord_status']['whyHeld'] = whyHeld
    
    def handle_error(self, id, errorCode, errorMsg):
        try:
            _ = self.orders[id]
        except KeyError:        
            self.orders[id]= {'ord_status':{}, 'error':{}, 'order':{}, 'contract': {}}
            
        self.orders[id]['error'] = {'errorCode': errorCode, 'errorMsg': errorMsg}
        
    def handle_open_order(self, orderId, state, order, contract):
        
        
        
        try:
            _ = self.orders[orderId]
        except KeyError:
            self.orders[orderId]= {'ord_status':{}, 'error':{}, 'order':{}, 'contract': {}, 'state':{}} 
        self.orders[orderId]['order'] = order
        self.orders[orderId]['contract'] = contract
        self.orders[orderId]['state'] = state
        
    def update(self, event, **param): 
        if event == 'orderStatus':
            self.handle_order_status(**param)
        elif event == 'openOrder':
            self.open_order = True
            self.handle_open_order(**param)
        elif event == 'openOrderEnd':
            self.open_order = False
        elif event == 'error':
            try:
                id = param['id']
                if id <> -1:
                    self.handle_error(**param)
            except:
                logging.error('OrderBook ERROR: in processing tws error event %s' % param)
                return
    
    def get_order_status(self, orderId):
        try:
            orderId = int(orderId)
            return self.orders[orderId]
        except:
            return None
   
   
    def get_open_orders(self):
        
        
        def filter_order_by_type(id_ord):
            try:
                if id_ord[1]['ord_status']['status'] in OrderBook.OPEN_ORDER_STATUS:
                    return id_ord[0] 
            except:
                pass
            return None
        
        try:
            i = 0
            while self.open_order:
                sleep(0.5)
                i += 1
                logging.warn('OrderBook: waiting open_order status to change to finish: round # %d... ' % i)
                if i == 10:
                    raise OrderManagerException
            res = map(filter_order_by_type, [(id, orders) for id, orders in self.orders.iteritems()])
            return res
            
            
            
        except:
            logging.error('OrderBook: get_open_orders exception %s' % traceback.format_exc())
            return None
    
               
           
          
'''
    client side order manager
'''
class OrderManager(BaseMessageListener):
    
    def __init__(self, name, gw_parent, kwargs):
        self.name = name
        self.gw_parent = gw_parent
        self.tws_connect = self.gw_parent.get_tws_connection()
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        self.order_book = OrderBook(self.name)

        self.order_id_mgr = OrderIdManager(self.tws_connect)
        self.tws_event_handler.set_order_id_manager(self.order_id_mgr)
        
        
        '''
             ask tws_event_handler to forward order messages to
             the order_book class
        '''
        for e in TWS_event_handler.PUBLISH_TWS_EVENTS:
            self.tws_event_handler.register(e, self.order_book)        
        
    
    def start_order_manager(self):
        self.order_id_mgr.start()
        '''
            
            rebuild order book
            **** 
             
             persitence function missing!!
             to be implemented
            
        '''
        self.tws_connect.reqOpenOrders()
        logging.info('OrderManager: start_order_manager: request open orders')
        
    
    def get_order_id_mgr(self):
        return self.order_id_mgr
    
    
    def get_order_book(self):
        return self.order_book
    
    def is_id_in_order_book(self, id):
        if self.get_order_book().get_order_status(id):
            return True
        return False
      
if __name__ == '__main__':
    
    oim = OrderIdManagerFake(1)

    def do_stuff(client):
        for i in range(10):
            oim.on_id_request(client, str(uuid.uuid4()))
            sleep(np.random.uniform(0, 1) * 0.5)
    
    for j in range(2):
        threading.Thread(target=do_stuff, args=(['client%d'% j])).start()
        #sleep(2)        
    
    oim.start()
        