#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from misc2.observer import Publisher, Subscriber
import json
from Queue import Queue
import threading
from datetime import datetime
from py4j.tests.java_gateway_test import sleep
from redis import Redis



class ApiMessageSinkException(Exception):
    pass


class ApiMessageSink(threading.Thread, Publisher):


    '''
        the API message sink is designed to receive messages that are being
        forwarded to here from rest API classes 
        Example of messages that are sent to the sink include order requests 
        received by the REST API classes from external clients.
        Messages received are stored in an internal queue and then
        get broadcasted to any other class that wants to consume these messages
        
        ApiMessagePersistence is a subscriber of these events with a purpose
        to store the events permanently in the redis store
        
        TelegramApiMessageAlert is another subscriber that forwards messages
        to the telegram bot so any one that has access to the bot can get
        real time alerts of orders placed by a client
    '''
    ON_API_MESSAGE = 'on_api_message'
    API_MESSAGES = [ON_API_MESSAGE] 

    def __init__(self, kwargs):
        threading.Thread.__init__(self)   
        Publisher.__init__(self, ApiMessageSink.API_MESSAGES) 
        self.msg_q = Queue()
        self.stop = False
        self.kwargs = kwargs
        
        
    def add_message(self, event, source, message):
        
        now= datetime.today().strftime('%Y%m%d %H:%M:%S')
        self.msg_q.put({'event': event, 'source': source, 'message': message, 'ts': now})
        self.msg_q.task_done()

    def set_stop(self):
        self.stop= True
        logging.info('ApiMessageSink: stopping....')
    
    def run(self):
        while not self.stop:
            if not self.msg_q.empty():
                
                m = self.msg_q.get()
                del(m['event'])
                self.dispatch('on_api_message', m)
                

                 
            sleep(0.5)


class BaseApiMessageSubscribe(Subscriber):
    def __init__(self, api_sink):
        self.api_sink = api_sink
        for e in ApiMessageSink.API_MESSAGES:
            self.api_sink.register(e, self, self.on_api_message)

    def on_api_message(self, event, source, message, ts):
        raise ApiMessageSinkException('Override this function in the inherited class!')


class ApiMessagePersistence(BaseApiMessageSubscribe):
    
    def __init__(self, rs, kwargs, api_sink):
        BaseApiMessageSubscribe.__init__(self, api_sink)
        self.kwargs = kwargs
        self.rs = rs
        try:
            self.list_label = '%d-%s' % (self.kwargs['tws_app_id'], self.kwargs['restapi.list_label'])
        except KeyError:
            self.list_label = '%d-%s' % (self.kwargs['tws_app_id'], 'api_log')
         
    
    def on_api_message(self, event, source, message, ts):
    #def on_api_message(self, **vars):
        self.rs.rpush(self.list_label, [event, source, message, ts])

from telegram.ext import Updater
from telegram.ext import CommandHandler

class TelegramApiMessageAlert(BaseApiMessageSubscribe):

    def __init__(self, access_token, api_sink):
        BaseApiMessageSubscribe.__init__(self, api_sink)
        updater = Updater(token=access_token)
        dispatcher = updater.dispatcher
        start_handler = CommandHandler('start', self.start)
        dispatcher.add_handler(start_handler)
        updater.start_polling()
        self.bot = None
    
    def start(self, bot, update):
        bot.send_message(chat_id=update.message.chat_id, text="ordm alert bot ready.")
        self.bot = bot
        self.chat_id = update.message.chat_id
        

    def send_message(self, message):
        print message
        if self.bot:
            self.bot.send_message(chat_id=self.chat_id, text=message)
  


    def on_api_message(self, event, source, message, ts):
        self.send_message('[%s%s] %s' % (ts, source, message))


# from websocket_server import WebsocketServer
# from ws.ws_server import BaseWebSocketServerWrapper
# 
# class APIWebSocketServer(BaseWebSocketServerWrapper, Subscriber):
#     
#     def __init__(self, name, kwargs):
#         
#         BaseWebSocketServerWrapper.__init__(self, name, kwargs)
#         self.clients = {}
# 
#     def loop_forever(self):
#         pass
#         
#     def encode_message(self, event_type, content):        
#         return json.dumps({'event': event_type, 'value': content})
#             
#     def new_client(self, client, server):
#         BaseWebSocketServerWrapper.new_client(self, client, server)
#         self.clients[client['id']] = client
#         self.clients[client['id']]['request'] = {}
#         
#         
#         server.send_message(client, '%s' % client)
#     
#     # Called for every client disconnecting
#     def client_left(self, client, server):
#         BaseWebSocketServerWrapper.client_left(self, client, server)
#         del self.clients[client['id']]
#         
#    
#     # Called when a client sends a message1
#     def message_received(self, client, server, message):
#         BaseWebSocketServerWrapper.message_received(self, client, server, message)
#         print '%s %s %s' % (client, server, message)
#         
# 
#     def handle_tws_event(self, **param):
#         print "APIWebSocketServer received tws events forwarded by gw"
#             
#     def update(self, event, **param): 
#         if event == 'orderStatus':
#             self.handle_tws_event(**param)
#         elif event == 'openOrder':
#             self.handle_tws_event(**param)
#         elif event == 'openOrderEnd':
#             self.handle_tws_event(**param)
#         elif event == 'error':
#             try:
#                 id = param['id']
#                 if id <> -1:
#                     self.handle_event(**param)
#             except:
#                 logging.error('OrderBook ERROR: in processing tws error event %s' % param)
#                 return
#             
#             
# 
# import websocket
# import thread, time
# from threading import Thread
# class APIClient():
#  
#     def __init__(self):
#          
#         self.ws = websocket.WebSocketApp("ws://localhost:9001",
#                                     on_message = self.on_message,
#                                     on_error = self.on_error,
#                                     on_close = self.on_close,
#                                     on_open = self.on_open)
#         #self.ws.on_open = self.on_open
#         self.stop = False
#         
# 
# 
#     def run_forever(self):
#         self.ws.run_forever()
#          
#     def on_message(self, message):
#         print 'apiclient recv %s' % message
#      
#     def on_error(self, error):
#         print error
#      
#     def on_close(self):
#         print "### closed ###"
#      
#      
#     def set_stop(self):
#         self.stop = True
#         
#     
#     def request_push_updates(self, request_type=None, fn_name=None ):
#         self.ws.send(json.dumps({'request_type': request_type, 'fn_name': fn_name}))
# 
#     def on_open(self): #, ws):
#         
#         
#         def run(*args):
#             while not self.stop:
#                 time.sleep(1)
#             self.ws.close()
#             print "thread terminating..."
#         thread.start_new_thread(run, ())
# 
# 
# 
# def order_status():
#     pass
#         
# if __name__ == '__main__':
# 
#     
# 
#     
#     logging.basicConfig(**{'level': logging.INFO,  'filemode': 'w', 'filename':'/tmp/api.log'}) 
#     logging.info('started...')
# 
# 
#     aws = APIWebSocketServer('aws', {'ws_port': 9001})
#      
#     def run_background():
#         aws.server.run_forever()
#              
#     t = Thread(target=run_background)        
#     t.start()    
#      
#     ac = APIClient()
#     def run_apiclient():
#         ac.run_forever()
#         
#     s = Thread(target=run_apiclient)        
#     s.start()    
#     
#     
#     f_info = {'order_status': order_status}
#     ac.request_push_updates('order_status', 'order_status')
# #     
# #     
# #     websocket.enableTrace(True)
# #     ws = websocket.WebSocketApp("ws://localhost:9001",
# #                               on_message = on_message,
# #                               on_error = on_error,
# #                               on_close = on_close)
# #     ws.on_open = on_open
# #     ws.run_forever()           
#     