from flask_restful import Resource, Api, reqparse
from misc2.helpers import ContractHelper, OrderHelper, OrderValidationException
from time import sleep
import uuid
import traceback
import json


class Commands(Resource):
    def get(self):
        return {'status': True, 'Available REST API' : {'exit': 'shutdown gateway', 
                                              'subscriptions': 'get a list of subscribed topics',
                                              'settings': 'get gateway startup settings',
                                               
                                              }
                }

'''
    function to force tws to return all open orders status
    this function will only cause open orders status to be updated in the 
    order manager
    
    this function is intend to test the TWS api function only.
    clients that call this function would not be able to identify
    what orders have their status returned from TWS. A higher level
    function call (synchronized call) should be designed to retrieve the 
    latest status from the order book instead
    
    
'''
class OpenOrdersStatus(Resource):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        
    
    def get(self):
        try:
            self.gw_conn.reqAllOpenOrders()
            return {}, 201
        except:

            return {'error': 'no order details found !' }, 404

'''
    function to retrieve the status of an order given its order id
    return 201 if record is found
    else return 404
'''
class OrderStatus(Resource):
    def __init__(self, webconsole): 
        self.wc = webconsole
        self.gw = self.wc.get_parent()
    
    def get(self, id):
        om = self.gw.get_order_manager()
        ob = om.get_order_book()
        status =  ob.get_order_status(id)
        if status:
            return status, 201
        else:
            return {'error': 'no order details found for id [%s]' % id}, 404
         
        

class OrderId(Resource):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw = self.wc.get_parent()
        

    '''
        function: checks whether the order id is in the order_id_manager given a clordid
                  this is used for checking the result after creating a order using the 
                  post method
        arguments:
            clordid should be unique across all clients (suggest to use uuid.uuid4) 
        http://localhost:5001/order_id?client_id=1000&clordid=779
    '''
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('clordid', required=True, help="clordid is required.")    
        args = parser.parse_args()
        iom = self.gw.get_order_id_manager()
        id = iom.assigned_id(args.get('clordid'))
        if id == None:
            return {}, 404
        return id, 200
        

    ''' 
        function: create a new order
        arguments:
            client_id is the application id
            clordid should be unique across all clients (suggest to use uuid.uuid4) 
        http://localhost:5001/order_id?client_id=1000&clordid=779
    '''
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('client_id', required=True, help="client_id is required.")
        parser.add_argument('clordid', required=True, help="clordid is required.")    
        args = parser.parse_args()
        iom = self.gw.get_order_id_manager()
        iom.request_id(args.get('client_id'), args.get('clordid'))
        return {}, 201
        

class AsyncOrderCRUD(Resource):


    def __init__(self, web_console):
        self.wc = web_console
        self.gw_conn = self.wc.get_parent().gw.get_tws_connection()

    '''
        
        create order
        arg: contract
             order_condition
    
    '''    
    def post(self, id):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('order_condition', required=True, help="order_condition is required.")
        args = parser.parse_args()
        js_contract = args.get('contract')
        contract = ContractHelper.kvstring2contract(js_contract)
        js_order_cond = args.get('order_condition')
        order = OrderHelper.kvstring2object(js_order_cond)
        self.gw_conn.placeOrder(int(id), contract, order)

    
    def get(self):
        pass
    
    
    def put(self):
        pass
    

    def delete(self):
        self.gw_conn.cancelOrder(int(id))
   
   
        
class SyncOrderCRUD(Resource):


    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        
    '''
        
        create order
        arg: contract
             order_condition
    
    '''    
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('order_condition', required=True, help="order_condition is required.")
        args = parser.parse_args()
        js_contract = args.get('contract')
        contract = ContractHelper.kvstring2contract(js_contract)
        js_order_cond = args.get('order_condition')
        clordid = str(uuid.uuid4())
        done = False
        iom = self.wc.get_parent().get_order_id_manager()
        iom.request_id('rest-api', clordid)
        id = None
        while not done:
            id = iom.assigned_id(clordid)
            if id != None:
                break
            sleep(0.5)
        
        try:    
            order = OrderHelper.kvstring2object(js_order_cond)
            OrderHelper.order_validation_ex(order)
            self.gw_conn.placeOrder(id['next_valid_id'], contract, order)
            return {'order id': id['next_valid_id']}, 201
        
        except OrderValidationException as e:
            return {'error': e.args[0]}, 409
        except ValueError:
            return {'error': 'check the format of the order message! %s' % traceback.format_exc()}, 409
    
    def get(self):
        pass
    
    
    def put(self):
        pass
    

    def delete(self):
        try:
            parser = reqparse.RequestParser()
            parser.add_argument('id', required=True, help="order id is required")
            args = parser.parse_args()
            id = int(args['id'])
            if self.wc.get_parent().get_order_manager().is_id_in_order_book(id):
                self.gw_conn.cancelOrder(int(id))
                return {'info': 'cancellation request sent. Check order status'}, 200
            else:
                return {'error': 'order id %d not found in order book' % id}, 404
                
        except:
            return {'error': 'cancel order failed: %s ' % traceback.format_exc()}, 404
            

class ExitApp(Resource):
    def __init__(self, webconsole):
        self.wc = webconsole
        
    def get(self):
        self.wc.post_shutdown()
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
