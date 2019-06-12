#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ormdapi.v2.ws.client.ws_api_client import RestStream
import logging, requests, time, json




def order_status(event, **order_status_dict):
    print order_status_dict
    
def price_quote(event, **quote_dict):
    print quote_dict    

def accepted(event, handle):
    print 'connection established %s %s' % (event, handle)

'''
    test api
'''
def test_quote(rest_server, handle):
    #url = 'http://%s/v2/quote?contract={"currency": "USD", "symbol": "EUR", "sec_type": "CASH", "exchange": "IDEALPRO"}&live_update=%s' % (rest_server, handle)
    url = 'http://%s/v2/quote?contract={"currency": "USD", "symbol": "GOOG", "sec_type": "OPT", "strike": 1077.5, "exchange": "SMART", "right": "C", "expiry":"20190614"}&live_update=%s' % (rest_server, handle)
    print url
    r = requests.get(url)
    print r.content
#         url = 'http://localhost:5001/v2/quote?contract={"currency": "HKD", "symbol": "HSI", "sec_type": "FUT", "exchange": "HKFE", "expiry":"20190627"}&live_update=%s' % handle
    
def test_new_order(rest_server, handle):
    url = 'http://%s/v2/order?contract={"right": "C", "exchange": "HKFE", "symbol": "HSI", "expiry": "20190627", "currency": "HKD", "sec_type": "OPT", "strike": 30400}&order_condition={"account": "U9050568", "order_type": "LMT", "what_if": "False", "price":150,"side": "SELL", "quantity": "1"}&live_update=%s' % (rest_server, handle)
    print url
    r = requests.post(url)
    id = None
    if r.status_code == 201:
        id = json.loads(r.content)['order_id']
        print 'order created. order_id %d' % id        
    if id:
        print 'next we are going to cancel this order...%d' % id
        time.sleep(2)
        test_cancel_order(rest_server, id)

def test_cancel_order(rest_server, order_id):
    url = 'http://%s/v2/order?id=%d' % (rest_server, order_id)
    r = requests.delete(url)
    print r.content
    
    
if __name__ == "__main__":
    logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
    )
    
    rest_server = 'ormd.vortifytech.com'
    ws_server = 'ws-ormd.vortifytech.com/ws'

    rs = RestStream(ws_server) 
    try:
        
        '''
            establish a websocket connection
            set up the callbacks 
        '''
        handle = rs.connect()
        rs.register(RestStream.RS_ORDER_STATUS, order_status)
        rs.register(RestStream.RS_QUOTE, price_quote)
        rs.register(RestStream.RS_ACCEPTED, accepted)    
        
        test_quote(rest_server, handle)
        test_new_order(rest_server, handle)

        # run a few seconds and quit 
        time.sleep(2)
        
    except KeyboardInterrupt:
        logging.warn('Received CTRL-C...terminating')
    finally:
        rs.disconnect()
        
    logging.info('exiting main()')   
        