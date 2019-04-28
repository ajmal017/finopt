from flask import Flask, jsonify

import json
import threading
from time import sleep
from misc2.observer import Subscriber
from flask_restful import Resource, Api, reqparse

import traceback
from ormdapi.v1 import apiv1
from ormdapi.v2 import apiv2




class WebConsole(Subscriber):

    
    app = Flask(__name__)
    api = Api(app)
    parser = reqparse.RequestParser()
    
    def __init__(self, parent=None):
        Subscriber.__init__(self, 'WebConsole' )
        self.parent = parent
        self.id_message = {}

    def get_parent(self):
        return self.parent
    
    def add_resource(self):
        WebConsole.api.add_resource(apiv1.Commands, '/v1')
        WebConsole.api.add_resource(apiv1.ExitApp, '/v1/exit', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv1.Subscriptions, '/v1/subscriptions', resource_class_kwargs={'gateway_instance': self.parent})
        WebConsole.api.add_resource(apiv1.GatewaySettings, '/v1/settings', resource_class_kwargs={'gateway_instance': self.parent})
        WebConsole.api.add_resource(apiv1.AsyncOrderCRUD, '/v1/async_order/<id>', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv1.SyncOrderCRUD, '/v1/order', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv1.OrderId, '/v1/order_id', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv1.OrderStatus, '/v1/order_status/<id>', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv1.OpenOrdersStatus, '/v1/open_orders', resource_class_kwargs={'webconsole': self})
        
        
        WebConsole.api.add_resource(apiv2.SyncOrderCRUD_v2, '/v2/order', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv2.OrderStatus_v2, '/v2/order_status/<id>', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv2.OpenOrdersStatus_v2, '/v2/open_orders', resource_class_kwargs={'webconsole': self})
        WebConsole.api.add_resource(apiv2.QuoteRequest_v2, '/v2/quote', resource_class_kwargs={'webconsole': self})



        
    def post_shutdown(self):
        self.parent.post_shutdown() 
    '''
        implement the consumer interface
        this function gets all tws events
        forwarded internally from tws_event_handler
    '''
    def update(self, event, **param):
        if event == 'error':
            print ('webconsole override %s: %s %s %s' % (self.name, event, "<empty param>" if not param else param,
                                          
                                         '<none>' if not param else param.__class__))
