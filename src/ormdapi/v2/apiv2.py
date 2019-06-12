from flask_restful import Resource, Api, reqparse
from misc2.helpers import ContractHelper, OrderHelper, OrderValidationException
from misc2.observer import Publisher
from finopt.instrument import Symbol
from time import sleep
from ormdapi.v2.position_handler import AccountSummaryTags
from ormdapi.v2.ws.ws_api_server import ApiSocketServer
import uuid, logging
import traceback
import json
from threading import RLock    




class InterestedTags():
    

    
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
#                                     'm_commission': "commission",
#                                     'm_minCommission': "min_commission",
#                                     'm_maxCommission': "max_commission",
#                                     'm_commissionCurrency': "commission_currency",
                                    'm_warningText': "warning_text"},
                        'error': {'errorCode': 'error_code',
                                  'errorMsg': 'error_msg'},
                        
                        }
    
    
    
    ContractDetails_tags = {'contract_info': {
                                        'm_industry':'industry',
                                        'm_liquidHours':'liquid_hours',
                                        'm_marketName':'market_name',
#                                        'm_evMultiplier':'ev_multiplier',
#                                        'm_evRule':'ev_rule',
                                        'm_summary':'summary',
                                        'm_minTick':'min_tick',
                                        'm_contractMonth':'contract_month',
                                        'm_longName':'long_name',
                                        'm_timeZoneId':'time_zoneId',
#                                        'm_orderTypes':'order_types',
#                                        'm_category':'category',
                                        'm_tradingHours':'trading_hours',
#                                        'm_validExchanges':'valid_exchanges',
#                                        'm_underConId':'under_conId',
#                                        'm_subcategory':'subcategory',
                                        'm_priceMagnifier':'price_magnifier',   
                                },
                            'summary': {
                                        'm_tradingClass':'trading_class',
                                        'm_right':'right',
                                        'm_symbol':'symbol',
#                                        'm_conId':'con_id',
                                        'm_secType':'sec_type',
#                                        'm_includeExpired':'include_expired',
                                        'm_primaryExch':'primary_exch',
                                        'm_multiplier':'multiplier',
                                        'm_expiry':'expiry',
                                        'm_currency':'currency',
                                        'm_localSymbol':'local_symbol',
                                        'm_exchange':'exchange',
                                        'm_strike':'strike',                                
                                }
                                        
        }
    
    @staticmethod
    def filter_unwanted_tags(o_status):
        os = {}
        try:
            for k,v in InterestedTags.OrderStatus_tags['order'].iteritems():
                os[v] = o_status['order'][k]
            for k,v in InterestedTags.OrderStatus_tags['ord_status'].iteritems():
                os[v] = o_status['ord_status'][k]
        except:
            pass
        try:
            for k,v in InterestedTags.OrderStatus_tags['state'].iteritems():
                os[v] = o_status['state'][k]
        except:
            pass
        
        try:
            os['error'] = 'error_code:%d, error_msg:%s' % (o_status['error']['errorCode'], o_status['error']['errorMsg'])
        except KeyError:
            os['error'] = ''
        
        return os
    
    @staticmethod
    def filter_unwanted_ci_tags(c_info_list):
        def process_tags(c_info):
            ci = {}
            try:
                for k,v in InterestedTags.ContractDetails_tags['contract_info'].iteritems():
                    ci[v] = c_info[k]
                ci.pop('summary')
                ci['summary'] = {}
                for k,v in InterestedTags.ContractDetails_tags['summary'].iteritems():
                    ci['summary'][v] = c_info['m_summary'][k]
                
            except:
                pass
            return ci
        return map(process_tags, c_info_list['contract_info'])
    
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
        
        
        
        symbol = js_v2['symbol'] if 'symbol' in js_v2 else None
        currency= js_v2['currency'] if 'currency' in js_v2 else None
        sec_type = js_v2['sec_type'] if 'sec_type' in js_v2 else None
        exchange = js_v2['exchange'] if 'exchange' in js_v2 else None
        expiry= js_v2['expiry'] if 'expiry' in js_v2 else None
        right= js_v2['right'] if 'right' in js_v2 else None
        strike= js_v2['strike'] if 'strike' in js_v2 else None
        
        if symbol == None or currency == None or sec_type == None or exchange == None:
            raise ('one or more of the values in symbol, currency, duration, sec_type, exchange, end_date contains null value!')
        if sec_type.upper() in ['OPT']:
            if expiry == None or right == None or strike == None:
                raise('expiry or right or strike can not be null for options!')
            try:
                _ = float(strike)
            except:
                raise('parameter strike must be a number!')
        if sec_type.upper() in ['FUT'] and expiry == None:
            print 'expiry can not be null for futures'
        
        
        
        for k,v in js_v2.iteritems():
            if k in mmap:
                cdict[mmap[k]] = v
        return ContractHelper.kv2contract(cdict)
    

    @staticmethod
    def format_v2_str_to_order(order_v2str, margin_check=False):
        omap = {'order_type': 'm_orderType',
                'account': 'm_account',
                'side': 'm_action',
                'quantity': 'm_totalQuantity', 
                'price':'m_lmtPrice',
                'aux_price': 'm_auxPrice',
                'order_ref': 'm_orderRef',
                }        
    
        
        odict ={}
        js_v2 = json.loads(order_v2str)
        for k,v in js_v2.iteritems():
            if k in omap:
                odict[omap[k]] = v
                
        try:
            if margin_check:
                odict['m_whatIf'] = True
        except:
            pass
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
        parser.add_argument('live_update', required=False, help="obtain live udpates via the websocket")
        args = parser.parse_args()
        js_contract = args.get('contract')
        try:
            contract = v2_helper.format_v2_str_to_contract(js_contract)
        except:
            return {'error': 'check the format of the contract string! %s' % traceback.format_exc()}, 409
        js_order_cond = args.get('order_condition')
        clordid = str(uuid.uuid4())

        self.wc.get_api_sink().add_message('/order', 'SyncOrderCRUD_v2:post', 'received new order %s condition: %s' % (js_contract, js_order_cond))
        logging.info('SyncOrderCRUD_v2:post received new order %s condition: %s' % (js_contract, js_order_cond))
        '''
           to do FIX error handling above ^^^ 
        '''
        if args['live_update']:
            handle_id = args['live_update'].strip('"').strip("'")
            result = self.wc.get_parent().get_ws_manager().register_request(handle_id, ApiSocketServer.RS_ORDER_STATUS)
            

        
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
            return {'order_id': id['next_valid_id']}, 201
        
        except OrderValidationException as e:
            return {'error': e.args[0]}, 409
        except:
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
        self.e_mkt_data = 'reqMktData'
        Publisher.__init__(self, [self.e_mkt_data])
        self.register(self.e_mkt_data, self.contract_mgr, callback=getattr(self.contract_mgr, self.e_mkt_data))
    
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('greeks', required=False, help="obtain option greeks")
        parser.add_argument('live_update', required=False, help="obtain live udpates via the websocket")
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
        
        


        try:
            contract = v2_helper.format_v2_str_to_contract(args['contract'])
            require_greeks = False
            
            if contract.m_secType in ['OPT']:
                if args['greeks'] <> None and args['greeks'].upper() == 'TRUE':
                    require_greeks = True
        except:
            return {'error': 'check the format of the contract message! %s' % traceback.format_exc()}, 409
        
        if args['live_update']:
            handle_id = args['live_update'].strip('"').strip("'")
            result = self.wc.get_parent().get_ws_manager().register_request(handle_id, ApiSocketServer.RS_QUOTE)
        '''
           to do FIX error handling above ^^^ 
        '''
        
                          
        sym = self.quote_mgr.get_symbol(contract)
        if sym:
            try:
                return output_result(sym, require_greeks), 200
            except:
                return 'invalid request! The supplied contract is found in the database but its definition could be invalid and rejected by the server!', 404
                    
        else:
            logging.info('QuoteRequest_v2: contract %s' %  ContractHelper.contract2kvstring(contract))
            self.dispatch(self.e_mkt_data, {'contract': ContractHelper.contract2kvstring(contract), 'snapshot': False})
            i = 0
#             while 1:
#                 sym =  self.quote_mgr.get_symbol(contract)
#                 if sym:
#                     break
#                 sleep(0.1)
#                 i += 0.5 
#                 if i >= 15:
#                     return 'Not getting any quotes from the server after waited 5 seconds! Contact administrator', 404
#                 
#             return output_result(sym, require_greeks), 200
        
            
            done = False       
            try:
                while not done:
                    sleep(0.1)
                    i += 0.5 
                    if i >= 15:
                        return 'Not getting any quotes  from the server after waited 10 seconds! Contact administrator', 404
                    sym =  self.quote_mgr.get_symbol(contract)
                    if sym <> None:
                        try:
                            _ = sym.get_contract()
                            return output_result(sym, require_greeks), 200
                        except:
                            return sym, 409
                    
            except:
                return {'error': 'check the format of the contract message! %s' % traceback.format_exc()}, 409         
        

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
            
            # reqPositions must be called first as the get_positions method
            # in AccountPositionTracker relies on the positionEnd flag to be 
            # set True 
            self.gw_conn.reqPositions()
            self.gw_conn.reqAccountSummary(self.reqId, "All", AccountSummaryTags.get_all_tags())
            self.gw_conn.reqAccountUpdates(True, '') #args['account']) 
#            return self.pm.get_positions(args['account']), 201 
            return self.pm.get_positions(), 201  ### the API doesn't support passing account as a param
            
        except KeyError:
            return self.pm.get_positions(), 201

        except:
            
            return {'error': 'AcctPosition_v2: %s' % traceback.format_exc()}, 409
   
   
   
class SystemStatus_v2(Resource):        

    def __init__(self, webconsole):
        self.wc = webconsole

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('last_error', required=False)
        parser.add_argument('num_lines', required=False)
        args = parser.parse_args()
        try:        
            if args['last_error'] != None:
                try:
                    nl = int(args['num_lines'])
                except:
                    nl = None
                return self.wc.retrieve_logs(nl), 200
                
            else:
                '''
                    return connectivity status
                '''
                return {'TWS connection status:': 'Connected' if self.wc.get_parent().get_ib_conn_status() else 'Disconnected. Wait for retry...'}, 200

        except:
            
            return {'error': 'SystemStatus: %s' % traceback.format_exc()}, 404
        
        return self.wc.retrieve_logs(), 200
        

class PreOrderMarginCheck_v2(Resource):


    def __init__(self, webconsole):
        self.wc = webconsole
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        self.om = self.wc.get_parent().get_order_manager()
    '''
        

    '''    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('order_condition', required=True, help="order_condition is required.")
        args = parser.parse_args()
        js_contract = args.get('contract')
        '''
            set the margin check flag to true
        '''
        contract = v2_helper.format_v2_str_to_contract(js_contract)
        js_order_cond = args.get('order_condition')
        clordid = str(uuid.uuid4())

        self.wc.get_api_sink().add_message('/order', 'PreOrderMarginCheck_v2:get', 'received new order %s condition: %s' % (js_contract, js_order_cond))
        
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
            order = v2_helper.format_v2_str_to_order(js_order_cond, True)
            OrderHelper.order_validation_ex(order)
            self.gw_conn.placeOrder(id['next_valid_id'], contract, order)
            i = 0
            while 1:
                 
                ob = self.om.get_order_book()
                status =  ob.get_order_status(id['next_valid_id'])
                if status:
                    return InterestedTags.filter_unwanted_tags(status), 201
                sleep(0.1)
                i += 0.5 
                if i >= 15:
                    return 'Not getting any margin information from the server after waited 5 seconds! Contact administrator', 404            
            
        except OrderValidationException as e:
            return {'error': e.args[0]}, 409
        except:
            return {'error': 'check the format of the margin check message! %s' % traceback.format_exc()}, 409
        
            
   


class ContractInfo_v2(Resource):
    CONTRACTINFO_REQID_START = 3000
    req_id = CONTRACTINFO_REQID_START
    def __init__(self, webconsole):
        self.wc = webconsole    
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        self.contract_info_mgr = self.wc.get_parent().get_contract_info_manager()
        self.lock = RLock()

    def get_req_id(self):
        try:
            dispatch = True
            self.lock.acquire()
            ContractInfo_v2.req_id += 1
        except:
            pass 
        finally:            
            self.lock.release()        
        return ContractInfo_v2.req_id
                    
    
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract_info', required=True, help="contract is required.")
        args = parser.parse_args()
        js_contract = args.get('contract_info')
        c = v2_helper.format_v2_str_to_contract(js_contract)
        id = self.get_req_id()
        self.gw_conn.reqContractDetails(id, c)
        done = False
        i=0
        try:
            while not done:
                sleep(0.1)
                i += 0.5 
                if i >= 20:
                    return 'Not getting any contract information from the server after waited 10 seconds! Contact administrator', 404
                cd = self.contract_info_mgr.get_contract_details(id)
                if cd <> None:
                    try:
                        _ = cd['contract_info']
                        return {'contract_info': InterestedTags.filter_unwanted_ci_tags(cd)}, 200
                    except:
                        return cd, 409
                
        except:
            return {'error': 'check the format of the contract message! %s' % traceback.format_exc()}, 409 
        



class HistoricalData_v2(Resource):
    HISTDATA_REQID_START = 4000
    req_id = HISTDATA_REQID_START
    def __init__(self, webconsole):
        self.wc = webconsole    
        self.gw_conn = self.wc.get_parent().get_tws_connection()
        self.contract_info_mgr = self.wc.get_parent().get_contract_info_manager()
        self.lock = RLock()

    def get_req_id(self):
        try:
            dispatch = True
            self.lock.acquire()
            HistoricalData_v2.req_id += 1
        except:
            pass 
        finally:            
            self.lock.release()        
        return HistoricalData_v2.req_id
                    
    
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('contract', required=True, help="contract is required.")
        parser.add_argument('range', required=True, help="range is required.")
        args = parser.parse_args()
        js_contract = args.get('contract')
        range = json.loads(args.get('range'))
        c = v2_helper.format_v2_str_to_contract(js_contract)
        id = self.get_req_id()
        #
        #def reqHistoricalData(self, tickerId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate)
        self.gw_conn.reqHistoricalData(id, 
                                        c, 
                                        range['end_date'].encode('ascii'), 
                                        range['duration'].encode('ascii'), 
                                        range['bar_size'].encode('ascii'),
                                        range['what_to_show'].encode('ascii'), 
                                        range['incl_off_mkt_data'], 
                                        1)
            
        done = False
        i=0
        try:
            while not done:
                sleep(0.1)
                i += 0.5 
                if i >= 60:
                    return 'Not getting any contract information from the server after waited 10 seconds! Check for last errors', 404
                hd = self.contract_info_mgr.get_historical_data(id)
                if hd <> None:
                    if 'error' in hd:
                        return {'historical_data': hd}, 404
    
                    return {'historical_data': hd}, 200
                
        except:
            return {'error': 'check the format of the contract message! %s' % traceback.format_exc()}, 409 
        
        
        
        