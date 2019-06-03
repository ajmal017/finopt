#!/usr/bin/env python
# -*- coding: utf-8 -*-
from websocket_server import WebsocketServer
import threading, logging, time, traceback
import json
from misc2.observer import Subscriber
from misc2.helpers import ContractHelper
from uuid import uuid4
from threading import RLock    

# https://github.com/Pithikos/python-websocket-server
class ApiSocketServer(WebsocketServer, threading.Thread, Subscriber):
    RS_ORDER_STATUS = 'rs_order_status'
    RS_QUOTE = 'rs_quote'
    RS_ACCEPTED = 'rs_accepted'
    EVENTS = [RS_ORDER_STATUS, RS_QUOTE, RS_ACCEPTED] 
    
    def __init__(self, name, gw_parent, host, port):
        threading.Thread.__init__(self, name=name)
        WebsocketServer.__init__(self, port, host)
        self.set_fn_new_client(self.new_client)
        self.set_fn_client_left(self.client_left)
        self.set_fn_message_received(self.message_received)

        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()     
        
        '''
            stores the mapping between each uuid handle and a web socket client id
            
        '''   
        self.handle_client_map= {}
        '''
            stores the mapping between request message type and a web socket client
        '''
        self.cli_requests = {}
        
        Subscriber.__init__(self, self.name)
        for e in ['error', 'orderStatus', 'tickPrice', 'tickSize',
                          'tickOptionComputation']:
            self.tws_event_handler.register(e, self)
        self.lock = RLock()
            

        
        
    def encode_message(self, event, stuff):
        return json.dumps({'event': event, 'msg': stuff})

            
    def new_client(self, client, server):
        logging.info("New client connected and was given id %d" % client['id'])
        #self.send_message_to_all("Hey all, a new client has joined us")
        handle_id = str(uuid4())
        self.handle_client_map[handle_id] = client
        logging.info('ApiSocketServer: new_client handle->%s client->%s' % (handle_id, client))
        self.send_message(client, self.encode_message('rs_accepted', {'handle': handle_id}))
        
    
    # Called for every client disconnecting
    def client_left(self, client, server):
        logging.info("Client(%d) disconnected" % client['id'])
#         for h, c in self.handle_client_map.iteritems():
#             if c == client:
#                 self.handle_client_map.pop(h)
#                 break
        for m in self.cli_requests:
            try:
                self.cli_requests[m].remove(client)
            except ValueError:
                continue
                
                
                    
    # Called when a client sends a message
    def message_received(self, client, server, message):
        logging.info("Client(%d) said: %s" % (client['id'], message))
        ed = json.loads(message)
#         if ed['event'] == 'request':
#             self.handle_client_request(client, ed['param'])
            
    '''
        this function is called by any rest API class that wants
        streaming data via the websocket. The parameter handle_id
        is returned to the client the first time it connects to the server 
    '''     
    def register_request(self, handle_id, msg_type):
        
        
        def _register_request(handle_id, msg_type):
            try:
                
    
                if msg_type not in ApiSocketServer.EVENTS:
                    logging.error('ApiSocketServer:handle_client_request invalid client request type %s!' % msg_type)
                    #return {'error': 'ApiSocketServer:handle_client_request invalid client request type %s!' % msg_type}
                _ = self.cli_requests[msg_type]
                
            except KeyError:
                self.cli_requests[msg_type] = []
                
            try:
                c = self.handle_client_map[handle_id]
                if c not in self.cli_requests[msg_type]:
                    self.cli_requests[msg_type].append(c)
            except KeyError:
                logging.error('ApiSocketServer:invalid handle %s!' % handle_id)
                #return {'error': 'ApiSocketServer:invalid handle %s!' % handle_id}
                
 
        try:
            self.lock.acquire()            
            _register_request(handle_id, msg_type)
        except:
            logging.error('ApiSocketServer: register_request %s' % traceback.format_exc())
        finally:            
            self.lock.release()        
               

        
         
    '''
        API handle quote stream
    '''   
    def handle_tickprice(self, contract_key, field, price, canAutoExecute):
        logging.debug('ApiSocketServer:tickPrice')
        try:
            for c in self.cli_requests[ApiSocketServer.RS_QUOTE]:
                self.send_message(c, self.encode_message(ApiSocketServer.RS_QUOTE,
                                                               {'tick_info': 'tick_price',
                                                                'contract_key': contract_key,
                                                                'field': field,
                                                                'price': price}
                                                               ))
        except:
            '''
                callbacks are being fired by tws_event_handler even when
                the clients are long gone
                we want to skip processing these callbacks
            '''
            pass
        
    def handle_ticksize(self, contract_key, field, size):
        logging.debug('ApiSocketServer:ticksize')
        #c = ContractHelper.makeContractfromRedisKeyEx(contract_key)
        try:
            for c in self.cli_requests[ApiSocketServer.RS_QUOTE]:
                self.send_message(c, self.encode_message(ApiSocketServer.RS_QUOTE,
                                                               {'tick_info': 'tick_size',
                                                                'contract_key': contract_key,
                                                                'field': field,
                                                                'size': size}
                                                               ))
        except:
            pass

    def handle_tickgreeks(self, **params):
        logging.debug('ApiSocketServer:tickOptionComputation')
        pass
    
    def handle_orderstatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        logging.debug('ApiSocketServer:handle_orderstatus')
        try:
            for c in self.cli_requests[ApiSocketServer.RS_ORDER_STATUS]:
                self.send_message(c, self.encode_message(ApiSocketServer.RS_ORDER_STATUS,
                                            {'order_id':orderId, 'status': status, 'filled':filled, 
                                             'remaining':remaining, 'avg_fill_price':avgFillPrice, 
                                             'perm_id':permId,
                                             #'parent_id':parentId, 
                                             'last_fill_price':lastFillPrice, 
                                             #'client_id':clientId, 
                                             'why_held':whyHeld}))        
        except:
            pass
        
    def update(self, event, **param): 
        if event == 'tickPrice':
            self.handle_tickprice(**param)
        elif event == 'tickSize':
            self.handle_ticksize(**param)
        elif event == 'tickOptionComputation':
            self.handle_tickgreeks(**param)
        elif event == 'orderStatus':
            self.handle_orderstatus(**param)
'''
a simplified version of this program for testing can be found under ws/tests/client2.py and server2.py
'''
    
