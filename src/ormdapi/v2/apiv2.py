from flask_restful import Resource, Api, reqparse
from misc2.helpers import ContractHelper, OrderHelper, OrderValidationException
from misc2.observer import Publisher
from finopt.instrument import Symbol
from time import sleep
from ormdapi.v2.position_handler import AccountSummaryTags
import uuid
import traceback
import json





class InterestedTags():
    
    '''
    
    order state information is not processed at this time.
        
        m_status = ""
        m_initMargin = ""
        m_maintMargin = ""
        m_equityWithLoan = ""
        m_commission = float()
        m_minCommission = float()
        m_maxCommission = float()
        m_commissionCurrency = ""
        m_warningText = ""
    
    '''
    
    OrderStatus_tags = {'order': {'m_orderId': 'order_id',
                                  'm_clientId': 'client_id',
                                  'm_action': 'side',
                                  'm_totalQuantity': 'quantity',
                                  'm_orderType': 'order_type',
                                  'm_lmtPrice': 'price',
                                  'm_auxPrice': 'aux_price',
                                  'm_orderRef': 'order_ref'},
                        'ord_status': {'status': 'status',
                                       'filled': 'filled',
                                       'remaining': 'remaining',
                                       'avgFillPrice': 'avg_fill_price',    
                                       'permId': 'perm_id'},
                        'contract': {'m_right': 'right', 
                                     'm_exchange': 'exchange',
                                     'm_symbol': 'symbol',
                                     'm_currency': 'currency',
                                     'm_secType': 'sec_type',
                                     'm_strike': 'strike',
                                     'm_expirt': 'expiry'},
                        'state':{   'm_initMargin': "init_margin",
                                    'm_maintMargin': "maint_margin",
                                    'm_equityWithLoan': "equity_with_loan",
                                    'm_commission': "commission",
                                    'm_minCommission': "min_commission",
                                    'm_maxCommission': "max_commission",
                                    'm_commissionCurrency': "commission_currency",
                                    'm_warningText': "warning_text"},
                        'error': {'errorCode': 'error_code',
                                  'errorMsg': 'error_msg'},
                        
                        }
    
    
    
    @staticmethod
    def filter_unwanted_tags(o_status):
        os = {}
        for k,v in InterestedTags.OrderStatus_tags['order'].iteritems():
            os[v] = o_status['order'][k]
        for k,v in InterestedTags.OrderStatus_tags['ord_status'].iteritems():
            os[v] = o_status['ord_status'][k]
        for k,v in InterestedTags.OrderStatus_tags['state'].iteritems():
            os[v] = o_status['state'][k]
            
        try:
            os['error'] = 'error_code:%d, error_msg:%s' % (o_status['error']['errorCode'], o_status['error']['errorMsg'])
        except KeyError:
            os['error'] = ''
        
        return os
    
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
class OpenOrdersStatus_v2(Resource):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        self.om = self.wc.get_parent().get_order_manager()
        
    
    def get(self):
        try:
            ob = self.om.get_order_book()
            self.gw_conn.reqAllOpenOrders()
            res = ob.get_open_orders()
            
            def filter_tags(id):
                try:
                    order =  ob.get_order_status(id)
                    if order:
                        os = InterestedTags.filter_unwanted_tags(order)
                        return {id:os}
                except:
                    pass
                return None

            open_orders = filter(lambda x: x <> None, map(filter_tags, res))
        
            
            return open_orders, 201
        except:

            return {'error': 'Error getting open orders!'}, 404

'''
    function to retrieve the status of an order given its order id
    return 201 if record is found
    else return 404
'''
class OrderStatus_v2(Resource):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw = self.wc.get_parent()
    
    def get(self, id):
        om = self.gw.get_order_manager() 
        ob = om.get_order_book()
        try:
            status =  ob.get_order_status(id)
            if status:
                os = InterestedTags.filter_unwanted_tags(status)
                return os, 201
            else:
                return {'error': 'no order details found for id [%s]' % id}, 404
        except:
            return {'error': 'error getting order status for [%s]' % id}, 404
        

   
class v2_helper():
    @staticmethod
    def format_v2_str_to_contract(contract_v2str):
    
        mmap = {
                "symbol": "m_symbol", 
                "sec_type": "m_secType", 
                "right": "m_right", 
                "expiry": "m_expiry", 
                "currency": "m_currency", 
                "exchange": "m_exchange", 
                "strike": 'm_strike'}
        
        cdict ={}
        js_v2 = json.loads(contract_v2str)
        for k,v in js_v2.iteritems():
            if k in mmap:
                 cdict[mmap[k]] = v
        return ContractHelper.kv2contract(cdict)
    

    @staticmethod
    def format_v2_str_to_order(order_v2str):
        omap = {'order_type': 'm_orderType',
                'account': 'm_account',
                'side': 'm_action',
                'quantity': 'm_totalQuantity', 
                'price':'m_lmtPrice',
                'aux_price': 'm_auxPrice',
                'order_ref': 'm_orderRef'
                }        
    
        
        odict ={}
        js_v2 = json.loads(order_v2str)
        for k,v in js_v2.iteritems():
            if k in omap:
                 odict[omap[k]] = v
        return OrderHelper.kv2object(odict)
    

        
class SyncOrderCRUD_v2(Resource):


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
        contract = v2_helper.format_v2_str_to_contract(js_contract)
        js_order_cond = args.get('order_condition')
        clordid = str(uuid.uuid4())

        self.wc.get_api_sink().add_message('/order', 'SyncOrderCRUD_v2:post', 'received new order %s condition: %s' % (js_contract, js_order_cond))
        
        done = False
        iom = self.wc.get_parent().get_order_id_manager()
        iom.request_id('rest-api', clordid)
        id = None
        while not done:
            id = iom.assigned_id(clordid)
            if id != None:
                break
            sleep(0.1)
        
        try:    
            order = v2_helper.format_v2_str_to_order(js_order_cond)
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
            
            self.wc.get_api_sink().add_message('/order', 'SyncOrderCRUD_v2:delete', 'received delete order %d' % (id))
            
            
            if self.wc.get_parent().get_order_manager().is_id_in_order_book(id):
                self.gw_conn.cancelOrder(int(id))
                return {'info': 'cancellation request sent. Check order status'}, 200
            else:
                return {'error': 'order id %d not found in order book' % id}, 404
        except:
            return {'error': 'cancel order failed: %s ' % traceback.format_exc()}, 404
            

class QuoteRequest_v2(Resource, Publisher):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.contract_mgr = self.wc.get_parent().get_subscription_manager()
        self.quote_mgr = self.wc.get_parent().get_quote_manager()
        self.event = 'reqMktData'
        Publisher.__init__(self, [self.event])
        self.register(self.event, self.contract_mgr, callback=getattr(self.contract_mgr, self.event))
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('greeks', required=False, help="obtain option greeks")
        args = parser.parse_args()

        '''
            if the contract is already in quote_handler
                just read off the values from quote_handler and return
            else
                subscribe the contract by dispatching a request to subscription manager
                loop
                    check quote_handler for the contract
                    if found, return values
                    
                
        '''
        def output_result(sym, require_greeks):
            
            
            res =  {'asize': sym.get_tick_value(Symbol.ASKSIZE), 'ask': sym.get_tick_value(Symbol.ASK),
                    'bsize': sym.get_tick_value(Symbol.BIDSIZE), 'bid': sym.get_tick_value(Symbol.BID),
                    'last': sym.get_tick_value(Symbol.LAST), 'high': sym.get_tick_value(Symbol.LOW),
                    'close': sym.get_tick_value(Symbol.CLOSE)}
            
            if require_greeks:
                opt_fields = [(Symbol.BID_OPTION, 'bid_option'),
                              (Symbol.ASK_OPTION, 'ask_option'),
                              (Symbol.LAST_OPTION, 'last_option')]
                              
                for ofld in opt_fields:
                    option_greeks = {}
                    try:
                        option_greeks[ofld[1]] = sym.get_ib_option_greeks(ofld[0])
                        res.update(option_greeks) 
                    except:
                        continue                
                
                
            return res
        
        


        contract = v2_helper.format_v2_str_to_contract(args['contract'])
        require_greeks = False
        try:
            if contract.m_secType in ['OPT']:
                if args['greeks'].upper() == 'TRUE':
                    require_greeks = True
        except:
            pass
                                    
        sym = self.quote_mgr.get_symbol(contract)
        if sym:
            return output_result(sym, require_greeks), 200
                    
        else:
            print ContractHelper.contract2kvstring(contract)
            self.dispatch(self.event, {'contract': ContractHelper.contract2kvstring(contract), 'snapshot': False})
            i = 0
            while 1:
                sym =  self.quote_mgr.get_symbol(contract)
                if sym:
                    break
                sleep(0.1)
                i += 0.5 
                if i >= 15:
                    return 'Not getting any quotes from the server after waited 5 seconds! Contact administrator', 404
                
            return output_result(sym, require_greeks), 200
        
        
        

'''
    function to ....
    
'''
class AcctPosition_v2(Resource, Publisher):
    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        self.pm = self.wc.get_parent().get_pos_manager()
        self.reqId = 4567
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('account', required=False, help="specify account name or leave blank to return all accounts")
        args = parser.parse_args()        
        try:
            
            # reqPositions must be called as the get_positions method
            # in AccountPositionTracker relies on the positionEnd flag to be 
            # set True 
            self.gw_conn.reqPositions()
            self.gw_conn.reqAccountSummary(self.reqId, "All", AccountSummaryTags.get_all_tags())
            return self.pm.get_positions(args['account']), 201
            
        except KeyError:
            return self.pm.get_positions(), 201

        except:
            
            return {'error': 'AcctPosition_v2: %s' % traceback.format_exc()}, 409
        
        