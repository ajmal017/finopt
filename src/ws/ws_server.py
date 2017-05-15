from websocket_server import WebsocketServer
import logging, time, traceback
from threading import Thread
import logging, copy, sys
import json
from misc2.observer import NotImplementedException, Subscriber, Publisher
from rethink.table_model import AbstractTableModel
from comms.ibgw.base_messaging import BaseMessageListener, Prosumer
# https://github.com/Pithikos/python-websocket-server


class BaseWebSocketServerWrapper(Subscriber):
    
    DEFAULT_CONFIG = {
      'ws_port': 9001,
    }    
    
    def __init__(self, name, kwargs):
               
        self.kwargs = copy.copy(self.DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in kwargs:
                self.kwargs[key] = kwargs.pop(key)        
        self.kwargs.update(kwargs)
                      
        server = WebsocketServer(self.kwargs['ws_port'])
        self.set_server(server)
        server.set_fn_new_client(self.new_client)
        server.set_fn_client_left(self.client_left)
        server.set_fn_message_received(self.message_received)
        
        self.set_stop = False
        
        
    def set_server(self, server):
        self.server = server
        
    def get_server(self):
        return self.server       
    
    def start_server(self):
        def run_background():
            self.server.run_forever()
            
        self.t = Thread(target=run_background)        
        self.t.start()
        logging.info('BaseWebSocketServerWrapper:start_server. Server started. Awaiting clients on port %d...' % self.kwargs['ws_port'])
        logging.info('BaseWebSocketServerWrapper:start_server. *** Inherited server must implement loop_forever function ***')
        
        while self.set_stop <> True:
            self.loop_forever()
            
    
    def loop_forever(self):
        self.server.shutdown()
        self.set_stop = True
        raise NotImplementedException('BaseWebSocketServerWrapper:loop_forever. *** Inherited server must implement loop_forever function !!! ***')
        

                
    def stop_server(self):
        '''
            call this function to shutdown the ws server and change the set_stop flag to true
            to give the main thread loop a chance to exit
             
        '''
        def shut_down():
            logging.info ('BaseWebSocketServerWrapper:stop_server. stopping server...')
            self.server.shutdown()
            self.set_stop = True
            logging.info ('BaseWebSocketServerWrapper:stop_server. stopped.')
        Thread(target=shut_down).start()
        # use a different thread to invoke shutdown() 
        #shut_down()
            
            
    def new_client(self, client, server):
        logging.info ("BaseWebSocketServerWrapper:new_client. New client connected and was given id %d" % client['id'])
    
    # Called for every client disconnecting
    def client_left(self, client, server):
        logging.info ("BaseWebSocketServerWrapper:client_left. Client(%d) disconnected" % client['id'])
    
    
    # Called when a client sends a message1
    def message_received(self, client, server, message):
        logging.info ("BaseWebSocketServerWrapper:message_received. Client(%d)=> %s" % (client['id'], message))
        
        

class ExampleWebSocketServer(BaseWebSocketServerWrapper):
    
    def __init__(self, name, kwargs):
        
        BaseWebSocketServerWrapper.__init__(self, name, kwargs)
        self.clients = {}
        logging.info('To test, run this sample, start the web browser and open finopt/src/ws/client_g.html')

    def loop_forever(self):
        try:
            logging.info('ExampleWebSocketServer:loop_forever. Server started. Awaiting clients on port %d...' % self.kwargs['ws_port'])
            time.sleep(1.5)
        except KeyboardInterrupt:
            print 'Caught keyboard interrupt!'
            self.stop_server()

        #print 'sending stuff.. %s' % str(list(self.clients.iteritems()))
        #val = {"rows": [{"c": [{"v": None}, {"v": 0}, {"v": -1.0}, {"v": -1.0}, {"v": 0}, {"v": 0.10314003859558614}, {"v": 0.7826350260448144}, {"v": -0.8130331453280795}, {"v": 23200.0}, {"v": 199.0}, {"v": 13}, {"v": 200.0}, {"v": 203.0}, {"v": 9}, {"v": 0.1287553281467331}, {"v": -0.25764241296227725}, {"v": -3.9381700903769823}]}, {"c": [{"v": None}, {"v": 5}, {"v": 897.0}, {"v": 937.0}, {"v": 5}, {"v": 0.08665033676603795}, {"v": 0.7518657873829434}, {"v": -0.6491859913057388}, {"v": 23400.0}, {"v": 246.0}, {"v": 8}, {"v": 249.0}, {"v": 252.0}, {"v": 13}, {"v": 0.12467434512421184}, {"v": -0.30878588256514583}, {"v": -4.234706968036659}]}, {"c": [{"v": None}, {"v": 5}, {"v": 756.0}, {"v": 796.0}, {"v": 5}, {"v": 0.06917053429732452}, {"v": 0.7065398873108217}, {"v": -0.4568940724016388}, {"v": 23600.0}, {"v": 307.0}, {"v": 13}, {"v": 308.0}, {"v": 310.0}, {"v": 4}, {"v": 0.12166656748142592}, {"v": -0.3681170702764589}, {"v": -4.515692106261017}]}, {"c": [{"v": 668.0}, {"v": 8}, {"v": 643.0}, {"v": 651.0}, {"v": 15}, {"v": 0.15305605752615425}, {"v": 0.551878304534173}, {"v": -3.783418388352659}, {"v": 23800.0}, {"v": 370.0}, {"v": 13}, {"v": 377.0}, {"v": 380.0}, {"v": 4}, {"v": 0.11601208353248117}, {"v": -0.43234383475205207}, {"v": -4.626457472864576}]}, {"c": [{"v": 536.0}, {"v": 8}, {"v": 524.0}, {"v": 540.0}, {"v": 15}, {"v": 0.14488937693281984}, {"v": 0.496787463346073}, {"v": -3.6799128088230453}, {"v": 24000.0}, {"v": 454.0}, {"v": 13}, {"v": 459.0}, {"v": 463.0}, {"v": 4}, {"v": 0.11243933205263512}, {"v": -0.5040128056340836}, {"v": -4.709556903241067}]}, {"c": [{"v": 433.0}, {"v": 5}, {"v": 422.0}, {"v": 426.0}, {"v": 4}, {"v": 0.14133844893390238}, {"v": 0.4384634388814806}, {"v": -3.635465401601853}, {"v": 24200.0}, {"v": 545.0}, {"v": 8}, {"v": 554.0}, {"v": 559.0}, {"v": 4}, {"v": 0.10680389782080972}, {"v": -0.5813432653714156}, {"v": -4.609331667196781}]}, {"c": [{"v": 339.0}, {"v": 13}, {"v": 333.0}, {"v": 336.0}, {"v": 4}, {"v": 0.13701853367669384}, {"v": 0.3782401877904869}, {"v": -3.4689303296964726}, {"v": 24400.0}, {"v": 663.0}, {"v": 8}, {"v": 664.0}, {"v": 669.0}, {"v": 4}, {"v": 0.10412411926510369}, {"v": -0.6579560810486558}, {"v": -4.478018972534759}]}, {"c": [{"v": 263.0}, {"v": 13}, {"v": 258.0}, {"v": 261.0}, {"v": 9}, {"v": 0.13426029637185147}, {"v": 0.3198261486390377}, {"v": -3.2585790706722135}, {"v": 24600.0}, {"v": 808.0}, {"v": 5}, {"v": 770.0}, {"v": 810.0}, {"v": 5}, {"v": 0.1050354544615176}, {"v": -0.7238166080971229}, {"v": -4.352834698986921}]}, {"c": [{"v": 201.0}, {"v": 8}, {"v": 197.0}, {"v": 199.0}, {"v": 9}, {"v": 0.13217043393596928}, {"v": 0.2649097174462373}, {"v": -2.989063157888885}, {"v": 24800.0}, {"v": None}, {"v": 5}, {"v": 909.0}, {"v": 949.0}, {"v": 5}, {"v": 0.0}, {"v": 0.0}, {"v": 0.0}]}], "cols": [{"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}, {"type": "number", "id": "strike", "label": "strike"}, {"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}]}

        #msg = self.encode_message('update_chart', val)
        #map(lambda x: self.server.send_message(x[1], msg), list(self.clients.iteritems()))
        
    def encode_message(self, event_type, content):        
        return json.dumps({'event': event_type, 'value': content})
            
    def new_client(self, client, server):
        BaseWebSocketServerWrapper.new_client(self, client, server)
        self.clients[client['id']] = client
        #val = self.port_table.get_JSON()
        val = {"rows": [{"c": [{"v": "HSI-20170529-22800.0"}, {"v": "P"}, {"v": 0.0}, {"v": 0.0}, {"v": None}, {"v": None}, {"v": 0}, {"v": -0.04024630813325283}, {"v": -1.7799243104295854}, {"v": 8.412138275492712e-05}, {"v": -0.0}, {"v": -0.0}, {"v": 0.0}, {"v": None}, {"v": None}]}, {"c": [{"v": "HSI-20170529-23000.0"}, {"v": "P"}, {"v": 8355.96}, {"v": -2330.45}, {"v": 167.11919999999998}, {"v": 23.0}, {"v": -2}, {"v": -0.054214984669962725}, {"v": -2.165483086658433}, {"v": 0.00011149281275378574}, {"v": 5.4214984669962725}, {"v": 216.54830866584328}, {"v": -0.011149281275378573}, {"v": 14411.919999999998}, {"v": 86.23736829759837}]}, {"c": [{"v": "HSI-20170529-24000.0"}, {"v": "P"}, {"v": 23119.04}, {"v": 5749.57}, {"v": 462.3808}, {"v": 113.0}, {"v": 1}, {"v": -0.2442512266651272}, {"v": -4.805393762378209}, {"v": 0.00040825153129837063}, {"v": -12.21256133325636}, {"v": -240.26968811891044}, {"v": 0.02041257656491853}, {"v": -17469.04}, {"v": -75.56126898002685}]}, {"c": [{"v": "HSI-20170529-25200.0"}, {"v": "C"}, {"v": 4255.96}, {"v": -7419.63}, {"v": 85.1192}, {"v": 75.0}, {"v": -2}, {"v": 0.18638198439938397}, {"v": -4.011529811754211}, {"v": 0.0003598537239552941}, {"v": -18.638198439938396}, {"v": 401.1529811754211}, {"v": -0.03598537239552941}, {"v": 1011.9200000000001}, {"v": 11.888269626594239}]}, {"c": [{"v": "HSI-20170529-25600.0"}, {"v": "C"}, {"v": 0.0}, {"v": 0.0}, {"v": None}, {"v": None}, {"v": 0}, {"v": 0.07654100802547596}, {"v": -2.1203024775561228}, {"v": 0.000195519195412408}, {"v": 0.0}, {"v": -0.0}, {"v": 0.0}, {"v": None}, {"v": None}]}, {"c": [{"v": "HSI-20170629-22800.0"}, {"v": "P"}, {"v": 4230.96}, {"v": -4402.12}, {"v": 84.6192}, {"v": 88.0}, {"v": -1}, {"v": -0.12450699761007489}, {"v": -2.5371124740419444}, {"v": 0.0001477103166999361}, {"v": 6.225349880503744}, {"v": 126.85562370209722}, {"v": -0.007385515834996806}, {"v": -169.03999999999996}, {"v": -3.995310756896764}]}, {"c": [{"v": "HSI-20170629-23800.0"}, {"v": "P"}, {"v": 13680.96}, {"v": -13074.68}, {"v": 273.6192}, {"v": 258.0}, {"v": -1}, {"v": -0.32294218710038886}, {"v": -3.796956413589867}, {"v": 0.0003009096584751097}, {"v": 16.147109355019442}, {"v": 189.84782067949334}, {"v": -0.015045482923755484}, {"v": 780.9599999999991}, {"v": 5.708371342361929}]}, {"c": [{"v": "HSI-20170629-24800.0"}, {"v": "C"}, {"v": 14969.04}, {"v": 13624.9}, {"v": 299.3808}, {"v": 274.0}, {"v": 1}, {"v": 0.3515224095508836}, {"v": -3.960105872459877}, {"v": 0.00031244564447224744}, {"v": 17.57612047754418}, {"v": -198.00529362299386}, {"v": 0.015622282223612373}, {"v": -1269.0400000000009}, {"v": -8.477764773158473}]}, {"c": [{"v": "HSI-20170629-25400.0"}, {"v": "C"}, {"v": 5014.29333335}, {"v": -16915.71}, {"v": 100.285866667}, {"v": 115.0}, {"v": -3}, {"v": 0.18560741085495613}, {"v": -2.779141408219208}, {"v": 0.00023104530983164467}, {"v": -27.84111162824342}, {"v": 416.87121123288125}, {"v": -0.034656796474746704}, {"v": -2207.1199999500013}, {"v": -14.67219043123833}]}], "cols": [{"type": "string", "id": "symbol", "label": "Symbol"}, {"type": "string", "id": "right", "label": "Right"}, {"type": "number", "id": "avgcost", "label": "Avg Cost"}, {"type": "number", "id": "market_value", "label": "Market Value"}, {"type": "number", "id": "avgpx", "label": "Avg Price"}, {"type": "number", "id": "spotpx", "label": "Spot Price"}, {"type": "number", "id": "pos", "label": "Quantity"}, {"type": "number", "id": "delta", "label": "Delta"}, {"type": "number", "id": "theta", "label": "Theta"}, {"type": "number", "id": "gamma", "label": "Gamma"}, {"type": "number", "id": "pos_delta", "label": "P. Delta"}, {"type": "number", "id": "pos_theta", "label": "P. Theta"}, {"type": "number", "id": "pos_gamma", "label": "P. Gamma"}, {"type": "number", "id": "unreal_pl", "label": "Unreal P/L"}, {"type": "number", "id": "percent_gain_loss", "label": "% gain/loss"}], "ckey_to_row_index": {}}
        server.send_message_to_all(self.encode_message('init_chart', val))

    
    # Called for every client disconnecting
    def client_left(self, client, server):
        BaseWebSocketServerWrapper.client_left(self, client, server)
        del self.clients[client['id']]
        
    
    # Called when a client sends a message1
    def message_received(self, client, server, message):
        BaseWebSocketServerWrapper.message_received(self, client, server, message)
        if len(message) > 200:
            message = message[:200]+'..'
        print("Client(%d) said: %s" % (client['id'], message))
        if message == 'quit':
            print 'shutting down...'
            self.stop_server()
            
            


            

class PortfolioTableModelListener(BaseMessageListener):
    '''
    
    '''
    CACHE_MAX = 15
    TIME_MAX = 1.0
    
    def __init__(self, name, server_wrapper):
        BaseMessageListener.__init__(self, name)
        self.mwss = server_wrapper
        self.simple_caching = {}

    def event_tm_table_cell_updated(self, event, source, row, row_values):
        logging.info("[%s] received %s content:[%d]" % (self.name, event, row))
        

        
    def event_tm_table_row_inserted(self, event, source, row, row_values):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))

    def event_tm_table_row_updated(self, event, source, row, row_values):   
        #logging.info("[%s] received %s content:[%s]" % (self.name, event, vars()))
        #logging.info("[%s] received %s content:[%d]" % (self.name, event, row))
        def notify_client():
            self.mwss.get_server().send_message_to_all(json.dumps(
                        {'event':event, 'value':{'row': row, 'row_values': row_values}}));
            
        try:
            curr_ts = time.time()
            if self.simple_caching[row]['count'] < PortfolioTableModelListener.CACHE_MAX or\
                curr_ts - self.simple_caching[row]['ts'] < PortfolioTableModelListener.TIME_MAX:
                self.simple_caching[row]['count'] +=1
                self.simple_caching[row]['ts'] = curr_ts
            else:
                logging.info('event_tm_table_row_updated: flush condition met, sending changes to clients. row:[%d] %d %0.2f' %
                                (row, self.simple_caching[row]['count'], curr_ts - self.simple_caching[row]['ts']))
                self.simple_caching[row]['count'] = 0
                self.simple_caching[row]['ts'] = curr_ts
                notify_client()
                
            
            
        except KeyError:
            self.simple_caching[row] = {'count': 1, 'ts': curr_ts}
            notify_client()    
    
    def event_tm_table_structure_changed(self, event, origin_request_id, account, data_table_json):
        try:
            logging.info("[%s] received %s content:[%d]" % (self.name, event, origin_request_id))
            self.mwss.get_server().send_message(self.mwss.clients[origin_request_id], 
                                                json.dumps({'event': event, 'value': data_table_json})) 
        #except IndexError, KeyError:
        except:
            logging.error('[%s]. index error %d' % (event, origin_request_id))
            

        


class MainWebSocketServer(BaseWebSocketServerWrapper):
    '''
    
        MainWebSocketServer
    
    '''
    def __init__(self, name, kwargs):
        BaseWebSocketServerWrapper.__init__(self, name, kwargs)
        
        '''
            clients:{
                     <client_id>: {'id': <WebsocketServer client {
                                                                    'id'      : client_id,
                                                                    'handler' : client_handler,
                                                                    'address' : (addr, port)
                                                                }
                                         >, 
                                   'request_info':{'account':<account>, 'tb_handler':<table model handling class name>
                                   
                    } 
        
        '''
#         topics = ['event_tm_table_cell_updated', 'event_tm_table_row_inserted', 
#                   'event_tm_table_row_updated', 'event_tm_table_structure_changed']
        topics = AbstractTableModel.TM_EVENTS
                
        self.message_handler = Prosumer(name='tblMessageHandler', kwargs={'bootstrap_host':'localhost', 'bootstrap_port':9092,
                                        'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                        'group_id': 'TMH1', 'session_timeout_ms':10000,
                                                 'topics': topics, 'clear_offsets' : False,
                                                 'seek_to_end': ['*']})
        tbl_listener = PortfolioTableModelListener('portTableModelListener', self)
        
        self.message_handler.add_listeners([tbl_listener])
        self.message_handler.start_prosumer()        
        self.clients = {}

        self.once = False
        
    def loop_forever(self):
        #logging.info('ChartTableWebSocketServer:loop_forever. Server started. Awaiting clients on port %d...' % self.kwargs['ws_port'])
        time.sleep(1.5)
#         if self.once == False:
#             #self.once = True
#             pass
            #print 'sending stuff.. %s' % str(list(self.clients.iteritems()))
            #val = {"rows": [{"c": [{"v": None}, {"v": 0}, {"v": -1.0}, {"v": -1.0}, {"v": 0}, {"v": 0.10314003859558614}, {"v": 0.7826350260448144}, {"v": -0.8130331453280795}, {"v": 23200.0}, {"v": 199.0}, {"v": 13}, {"v": 200.0}, {"v": 203.0}, {"v": 9}, {"v": 0.1287553281467331}, {"v": -0.25764241296227725}, {"v": -3.9381700903769823}]}, {"c": [{"v": None}, {"v": 5}, {"v": 897.0}, {"v": 937.0}, {"v": 5}, {"v": 0.08665033676603795}, {"v": 0.7518657873829434}, {"v": -0.6491859913057388}, {"v": 23400.0}, {"v": 246.0}, {"v": 8}, {"v": 249.0}, {"v": 252.0}, {"v": 13}, {"v": 0.12467434512421184}, {"v": -0.30878588256514583}, {"v": -4.234706968036659}]}, {"c": [{"v": None}, {"v": 5}, {"v": 756.0}, {"v": 796.0}, {"v": 5}, {"v": 0.06917053429732452}, {"v": 0.7065398873108217}, {"v": -0.4568940724016388}, {"v": 23600.0}, {"v": 307.0}, {"v": 13}, {"v": 308.0}, {"v": 310.0}, {"v": 4}, {"v": 0.12166656748142592}, {"v": -0.3681170702764589}, {"v": -4.515692106261017}]}, {"c": [{"v": 668.0}, {"v": 8}, {"v": 643.0}, {"v": 651.0}, {"v": 15}, {"v": 0.15305605752615425}, {"v": 0.551878304534173}, {"v": -3.783418388352659}, {"v": 23800.0}, {"v": 370.0}, {"v": 13}, {"v": 377.0}, {"v": 380.0}, {"v": 4}, {"v": 0.11601208353248117}, {"v": -0.43234383475205207}, {"v": -4.626457472864576}]}, {"c": [{"v": 536.0}, {"v": 8}, {"v": 524.0}, {"v": 540.0}, {"v": 15}, {"v": 0.14488937693281984}, {"v": 0.496787463346073}, {"v": -3.6799128088230453}, {"v": 24000.0}, {"v": 454.0}, {"v": 13}, {"v": 459.0}, {"v": 463.0}, {"v": 4}, {"v": 0.11243933205263512}, {"v": -0.5040128056340836}, {"v": -4.709556903241067}]}, {"c": [{"v": 433.0}, {"v": 5}, {"v": 422.0}, {"v": 426.0}, {"v": 4}, {"v": 0.14133844893390238}, {"v": 0.4384634388814806}, {"v": -3.635465401601853}, {"v": 24200.0}, {"v": 545.0}, {"v": 8}, {"v": 554.0}, {"v": 559.0}, {"v": 4}, {"v": 0.10680389782080972}, {"v": -0.5813432653714156}, {"v": -4.609331667196781}]}, {"c": [{"v": 339.0}, {"v": 13}, {"v": 333.0}, {"v": 336.0}, {"v": 4}, {"v": 0.13701853367669384}, {"v": 0.3782401877904869}, {"v": -3.4689303296964726}, {"v": 24400.0}, {"v": 663.0}, {"v": 8}, {"v": 664.0}, {"v": 669.0}, {"v": 4}, {"v": 0.10412411926510369}, {"v": -0.6579560810486558}, {"v": -4.478018972534759}]}, {"c": [{"v": 263.0}, {"v": 13}, {"v": 258.0}, {"v": 261.0}, {"v": 9}, {"v": 0.13426029637185147}, {"v": 0.3198261486390377}, {"v": -3.2585790706722135}, {"v": 24600.0}, {"v": 808.0}, {"v": 5}, {"v": 770.0}, {"v": 810.0}, {"v": 5}, {"v": 0.1050354544615176}, {"v": -0.7238166080971229}, {"v": -4.352834698986921}]}, {"c": [{"v": 201.0}, {"v": 8}, {"v": 197.0}, {"v": 199.0}, {"v": 9}, {"v": 0.13217043393596928}, {"v": 0.2649097174462373}, {"v": -2.989063157888885}, {"v": 24800.0}, {"v": None}, {"v": 5}, {"v": 909.0}, {"v": 949.0}, {"v": 5}, {"v": 0.0}, {"v": 0.0}, {"v": 0.0}]}], "cols": [{"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}, {"type": "number", "id": "strike", "label": "strike"}, {"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}]}

            #msg = self.encode_message('update_chart', val)
            #map(lambda x: self.server.send_message(x[1], msg), list(self.clients.iteritems()))
            
    def encode_message(self, event_type, content):        
        return json.dumps({'event': event_type, 'value': content})
            
    def new_client(self, client, server):
        self.clients[client['id']] = client 
        print 'new client id:%d %s' % (client['id'], client)
    
    # Called for every client disconnecting
    def client_left(self, client, server):
        try:
            del self.clients[client['id']]
        except:
            pass
        
    
    # Called when a client sends a message1
    def message_received(self, client, server, message):
        print 'message received %s' % message
        self.message_handler.send_message(AbstractTableModel.EVENT_TM_REQUEST_TABLE_STRUCTURE, 
                                          json.dumps({'request_id' : client['id'], 'account': 'U8379890'}))



    

def main():
    
    kwargs = {
      'ws_port': 9001,

    }       
    esw = MainWebSocketServer('ChartTableWS', kwargs)    
    esw.start_server()

    
    
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
    
