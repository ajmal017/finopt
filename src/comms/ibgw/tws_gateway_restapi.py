from flask import Flask
from flask_restful import Resource, Api, reqparse
import json
import threading
import time
from cheroot.server import Gateway
import sys


class WebConsole():

    app = Flask(__name__)
    api = Api(app)
    parser = reqparse.RequestParser()
    
    def __init__(self, parent=None):
        self.parent = parent

    def get_parent(self):
        return self.parent
    
    def add_resource(self):
        WebConsole.api.add_resource(Commands, '/')
        WebConsole.api.add_resource(ExitApp, '/exit', resource_class_kwargs={'gateway_instance': self.parent})
        WebConsole.api.add_resource(Subscriptions, '/subscriptions', resource_class_kwargs={'gateway_instance': self.parent})
        WebConsole.api.add_resource(GatewaySettings, '/settings', resource_class_kwargs={'gateway_instance': self.parent})
        

class Commands(Resource):
    def get(self):
        return {'status': True, 'Available REST API' : {'exit': 'shutdown gateway', 
                                              'subscriptions': 'get a list of subscribed topics',
                                              'settings': 'get gateway startup settings',
                                              
                                              }
                }

class ExitApp(Resource):
    def __init__(self, gateway_instance):
        self.gw = gateway_instance
        
    def get(self):
        self.gw.post_shutdown()
        return {'status': 'please check the log for exit status'}
        

class GatewaySettings(Resource):
    def __init__(self, gateway_instance):
        self.gw = gateway_instance
        
    def get(self):
        return json.loads(json.dumps(self.gw.kwargs))
    
    

    


class Subscriptions(Resource):
    def __init__(self, gateway_instance):
        self.gw = gateway_instance
    
    def get(self):
        idc = self.gw.contract_subscription_mgr.get_id_contracts()
        c2id = self.gw.contract_subscription_mgr.idContractMap['contract_id']
        return {'status': True, 'subscriptions' : {'id2c': idc, 'c2id': c2id}}


# def start_flask():
#     w = WebConsole()
#     w.add_resource()
#     w.app.run(debug=True, use_reloader=False)
# 
# if __name__ == '__main__':
#     
#     
#     
#     t_webApp = threading.Thread(name='Web App', target=start_flask)
#     t_webApp.setDaemon(True)
#     t_webApp.start()
#     
#     try:
#         while True:
#             print 'sleeping...'
#             time.sleep(1)
#     
#     except KeyboardInterrupt:
#         print("exiting")
#         exit(0)    
#     