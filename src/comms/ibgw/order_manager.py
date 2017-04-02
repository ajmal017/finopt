#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import strftime
import json
from misc2.helpers import ContractHelper
from ib.ext.Contract import Contract
from comms.ibgw.base_messaging import BaseMessageListener
from comms.ibgw.tws_event_handler import TWS_event_handler
from __main__ import name
from _ast import Name



class OrderBook():
    
    def __init__(self, name):
        '''
            orderbook:
                contains orders which is a dict that maps tws orderId 
                each order has an order_status map
                each order has an order_status_hist which is a list to previous order_status
                each order has an exec_status_hist which is a list of executions
            
                
        '''
        self.name = name 
        self.orders = {}
        
    def update_order_status(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        if orderId in self.orders:
            self.orders[orderId]['order_status']['status'] = status
            self.orders[orderId]['order_status']['filled'] = filled
            self.orders[orderId]['order_status']['remaining'] = remaining
            self.orders[orderId]['order_status']['avgFillPrice'] = avgFillPrice
            self.orders[orderId]['order_status']['permId'] = permId
            self.orders[orderId]['order_status']['parentId'] = parentId
            self.orders[orderId]['order_status']['lastFillPrice'] = lastFillPrice
            self.orders[orderId]['order_status']['clientId'] = clientId
            self.orders[orderId]['order_status']['whyHeld'] = whyHeld
        else:
            logging.warn('OrderBook:update_order_status. orderId %d key not found in the dict.' % orderId)
        

class OrderManager(BaseMessageListener):
    
    def __init__(self, name, tws_connection, producer, rs_conn, kwargs):
        self.name = name
        self.tws_connect = tws_connection
        self.order_book = OrderBook(self.name)
        self.order_id = self.load_order_id()
        
        
    def load_order_id(self):
        return -1
    
    def get_next_order_id(self):
        self.order_id = self.order_id + 1
        return self.order_id
    
    
    def place_order(self, event, client_order_id, contract_key, side, quantity, price):
        logging.info('OrderManager:place_order. client_order_id %d, contract %s, side %s, qty %d, price %8.2f' %
                            (client_order_id, contract_key, side, quantity, price))

        '''
            insert a new entry into the orderbook
            get the next order id
            submit order request to tws
        '''

                            
        try:
            vals = json.loads(value)
        except ValueError:
            logging.error('TWS_gateway - placeOrder Exception %s' % traceback.format_exc())
            return
        
    #        c = ContractHelper.kvstring2contract(vals[1])
        o = OrderHelper.kvstring2object(vals[2], Order)
        o.__dict__['transmit'] = self.ib_order_transmit
    #         print c.__dict__
    #         print o.__dict__
    #         print '---------------------'
    
           
        #self.connection.placeOrder(vals[0], ContractHelper.kvstring2contract(vals[1]), OrderHelper.kvstring2object(vals[2], Order))
        self.tws_connect.placeOrder(vals[0], ContractHelper.kvstring2object(vals[1], Contract), OrderHelper.kvstring2object(vals[2], Order))
    #        self.connection.placeOrder(orderId, contract, newOptOrder)
        

    
        
        