from flask import Flask
from flask_restful import Resource, Api, reqparse
import json
import threading
import time
import sys
from misc2.helpers import ContractHelper
import simplejson

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
        WebConsole.api.add_resource(ExitApp, '/exit', resource_class_kwargs={'pm_instance': self.parent})
        WebConsole.api.add_resource(RequestPosition, '/reqPosition', resource_class_kwargs={'pm_instance': self.parent})
        WebConsole.api.add_resource(RequestAccountUpdates, '/reqAccountUpdates', resource_class_kwargs={'pm_instance': self.parent})
        WebConsole.api.add_resource(GetTDS, '/TDS', resource_class_kwargs={'pm_instance': self.parent})        
        WebConsole.api.add_resource(GetPortfolio, '/Portfolio', resource_class_kwargs={'pm_instance': self.parent})
        WebConsole.api.add_resource(RerequestMarketData, '/marketdata', resource_class_kwargs={'pm_instance': self.parent})        
        

class Commands(Resource):
    def get(self):
        return {'status': True, 'Available REST API' : {'exit': 'shutdown PM', 
                                              'reqPosition': 'ask gateway to retrieve position from IB',
                                              'reqAccountUpdates': 'ask gateway to retrieve account updates from IB',
                                              'TDS': 'get TDS',
                                              'Portfolio' : 'display portfolio positions',
                                              'marketdata': 're-request market data'
                                              }
                }

class ExitApp(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        self.pm.post_shutdown()
        return {'status': 'please check the log for exit status'}
        

class RequestPosition(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        self.pm.twsc.reqPositions()
        return {'status': True}
    
class RequestAccountUpdates(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        self.pm.reqAllAcountUpdates()
        return {'status': True}

class RerequestMarketData(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        return {'status': True, 'marketdata': json.loads(json.dumps(self.pm.re_request_market_data()))}
    

class GetTDS(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        sl = map(lambda x: [ContractHelper.contract2kv(x['syms'][0].get_contract()), 
                            x['syms'][0].get_tick_values(), x['syms'][0].get_extra_attributes()], self.pm.tds.symbols.values())
        return json.loads(simplejson.dumps({'status': True, 'symbols': sl}, ignore_nan=True))


class GetPortfolio(Resource):
    def __init__(self, pm_instance):
        self.pm = pm_instance
        
    def get(self):
        try:
            def format_port_data(x):
                    return {'key': x[0], 'port_fields': x[1].get_port_fields(), 
                            'tick_fields': x[1].get_instrument().get_tick_values()}
            
            
            ports = {}
            for p in self.pm.portfolios.values():
                p.calculate_port_pl()
                data = map(format_port_data, [x for x in p.port['port_items'].iteritems()])
                ports[p.get_account()] = {'port_items': data, 'port_summary': p.port['port_v']}
        except:
            return {'status': False}
        return json.loads(simplejson.dumps({'status': True, 'ports': ports}, ignore_nan=True))



#     