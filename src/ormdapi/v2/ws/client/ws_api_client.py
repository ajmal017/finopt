#!/usr/bin/env python
# -*- coding: utf-8 -*-
import websocket
import threading
import time, json, traceback, logging, requests

class WsClient(websocket.WebSocketApp, threading.Thread):
    
    def __init__(self, parent, url):
        threading.Thread.__init__(self)  
        websocket.WebSocketApp.__init__(self,    
                              url = url,                           
                              on_message = self.on_message,
                              on_error = self.on_error,
                              on_close = self.on_close,
                              on_open = self.on_open)
        self.quit = False
        self.connected = False
        self.parent = parent
        self.assigned_handle = None
        
    def encode_message(self, stuff):
        return json.dumps({'msg': stuff})
    
    def on_message(self, message):
        logging.info("WsClient:on_message received a message from the server %s" % message)
        try:
            e = json.loads(message)
            if e['event'] == RestStream.RS_ACCEPTED:
                self.assigned_handle = e['msg']['handle']
            self.parent._dispatch(e['event'], e['msg'])
                
        except:
            logging.error('WsClient: on_message %s' % traceback.format_exc())
    
    def request_updates(self, event_name):
        self.send(json.dumps({'event': 'request', 'param': event_name}))
    
    def on_error(self, error):
        logging.info(error)
    
    def on_close(self):
        logging.info("WsClient:on_close ### closed ###")

    
    def on_open(self):
        logging.info('WsClient:on_open connection opened')
        self.connected = True
        
    def is_connected(self):
        return self.connected
    

    
    def shutdown(self):
        self.quit = True
        self.close()
        logging.info("WsClient:shutdown thread terminating...")
                

class RestStream():
    RS_ORDER_STATUS = 'rs_order_status'
    RS_QUOTE = 'rs_quote'
    RS_ACCEPTED = 'rs_accepted'
    EVENTS = [RS_ORDER_STATUS, RS_QUOTE, RS_ACCEPTED]
    
    def __init__(self, host, port):
        self.url = 'ws://%s:%d' % (host, port)
        self.events = { event : []
                          for event in RestStream.EVENTS }
        
    def connect(self):
        try:
            self.wsc = WsClient(self, self.url)
            t = threading.Thread(name='wsc', target=self.wsc.run_forever)
            t.setDaemon(True)
            t.start()         
            i = 0
            time.sleep(0.5)
            while not self.wsc.is_connected():
                i+=1
                time.sleep(0.5)
                if i > 5:
                    raise Exception('RestStream: connect timeout.')
            
            return self.get_assigned_handle()
        except:
            logging.error('RestStream:connect %s' % traceback.format_exc())
            raise Exception('RestStream: connect timeout.')
         
    
    def _get_subscribers(self, event):
        return self.events[event]

    def _dispatch(self, event, params=None):
        for callback in self._get_subscribers(event):
            callback(event, **params)
    
    def register(self, event, callback):
        self._get_subscribers(event).append(callback)
        self.wsc.request_updates(event)
    
    
    def unregister(self, event, callback):
        if callback in self._get_subscribers(event): 
            self._get_subscribers.remove(callback)
        
    def get_assigned_handle(self):
        if self.wsc:
            return self.wsc.assigned_handle    
        return None
    
    
    def disconnect(self):
        self.wsc.shutdown()
           
