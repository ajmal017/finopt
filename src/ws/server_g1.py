from websocket_server import WebsocketServer
import threading, logging, time, traceback
import json
# https://github.com/Pithikos/python-websocket-server


class WebSocketServerWrapper(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self, name=name)
        self.clients = {}
        
    def set_server(self, server):
        self.server = server
        
    def run(self):   
        print 'here'
        while 1:
            time.sleep(0.5)
#             print 'sending stuff.. %s' % str(list(self.clients.iteritems()))
            val = {"rows": [{"c": [{"v": None}, {"v": 0}, {"v": -1.0}, {"v": -1.0}, {"v": 0}, {"v": 0.10314003859558614}, {"v": 0.7826350260448144}, {"v": -0.8130331453280795}, {"v": 23200.0}, {"v": 199.0}, {"v": 13}, {"v": 200.0}, {"v": 203.0}, {"v": 9}, {"v": 0.1287553281467331}, {"v": -0.25764241296227725}, {"v": -3.9381700903769823}]}, {"c": [{"v": None}, {"v": 5}, {"v": 897.0}, {"v": 937.0}, {"v": 5}, {"v": 0.08665033676603795}, {"v": 0.7518657873829434}, {"v": -0.6491859913057388}, {"v": 23400.0}, {"v": 246.0}, {"v": 8}, {"v": 249.0}, {"v": 252.0}, {"v": 13}, {"v": 0.12467434512421184}, {"v": -0.30878588256514583}, {"v": -4.234706968036659}]}, {"c": [{"v": None}, {"v": 5}, {"v": 756.0}, {"v": 796.0}, {"v": 5}, {"v": 0.06917053429732452}, {"v": 0.7065398873108217}, {"v": -0.4568940724016388}, {"v": 23600.0}, {"v": 307.0}, {"v": 13}, {"v": 308.0}, {"v": 310.0}, {"v": 4}, {"v": 0.12166656748142592}, {"v": -0.3681170702764589}, {"v": -4.515692106261017}]}, {"c": [{"v": 668.0}, {"v": 8}, {"v": 643.0}, {"v": 651.0}, {"v": 15}, {"v": 0.15305605752615425}, {"v": 0.551878304534173}, {"v": -3.783418388352659}, {"v": 23800.0}, {"v": 370.0}, {"v": 13}, {"v": 377.0}, {"v": 380.0}, {"v": 4}, {"v": 0.11601208353248117}, {"v": -0.43234383475205207}, {"v": -4.626457472864576}]}, {"c": [{"v": 536.0}, {"v": 8}, {"v": 524.0}, {"v": 540.0}, {"v": 15}, {"v": 0.14488937693281984}, {"v": 0.496787463346073}, {"v": -3.6799128088230453}, {"v": 24000.0}, {"v": 454.0}, {"v": 13}, {"v": 459.0}, {"v": 463.0}, {"v": 4}, {"v": 0.11243933205263512}, {"v": -0.5040128056340836}, {"v": -4.709556903241067}]}, {"c": [{"v": 433.0}, {"v": 5}, {"v": 422.0}, {"v": 426.0}, {"v": 4}, {"v": 0.14133844893390238}, {"v": 0.4384634388814806}, {"v": -3.635465401601853}, {"v": 24200.0}, {"v": 545.0}, {"v": 8}, {"v": 554.0}, {"v": 559.0}, {"v": 4}, {"v": 0.10680389782080972}, {"v": -0.5813432653714156}, {"v": -4.609331667196781}]}, {"c": [{"v": 339.0}, {"v": 13}, {"v": 333.0}, {"v": 336.0}, {"v": 4}, {"v": 0.13701853367669384}, {"v": 0.3782401877904869}, {"v": -3.4689303296964726}, {"v": 24400.0}, {"v": 663.0}, {"v": 8}, {"v": 664.0}, {"v": 669.0}, {"v": 4}, {"v": 0.10412411926510369}, {"v": -0.6579560810486558}, {"v": -4.478018972534759}]}, {"c": [{"v": 263.0}, {"v": 13}, {"v": 258.0}, {"v": 261.0}, {"v": 9}, {"v": 0.13426029637185147}, {"v": 0.3198261486390377}, {"v": -3.2585790706722135}, {"v": 24600.0}, {"v": 808.0}, {"v": 5}, {"v": 770.0}, {"v": 810.0}, {"v": 5}, {"v": 0.1050354544615176}, {"v": -0.7238166080971229}, {"v": -4.352834698986921}]}, {"c": [{"v": 201.0}, {"v": 8}, {"v": 197.0}, {"v": 199.0}, {"v": 9}, {"v": 0.13217043393596928}, {"v": 0.2649097174462373}, {"v": -2.989063157888885}, {"v": 24800.0}, {"v": None}, {"v": 5}, {"v": 909.0}, {"v": 949.0}, {"v": 5}, {"v": 0.0}, {"v": 0.0}, {"v": 0.0}]}], "cols": [{"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}, {"type": "number", "id": "strike", "label": "strike"}, {"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}]}

            msg = self.encode_message('update_chart', val)
            map(lambda x: self.server.send_message(x[1], msg), list(self.clients.iteritems()))
            
    def encode_message(self, event_type, content):
        
        return json.dumps({'event': event_type, 'value': content})
            
    def new_client(self, client, server):
        print("New client connected and was given id %d" % client['id'])
        self.clients[client['id']] = client
#        server.send_message_to_all("Hey all, a new client has joined us")
        #val = {"cols":[{"id":"task","label":"Task","type":"string"},{"id":"hours","label":"Hours per Day","type":"number"}],"rows":[{"c":[{"v":"Work"},{"v":11}]},{"c":[{"v":"Eat"},{"v":2}]},{"c":[{"v":"Commute"},{"v":2}]},{"c":[{"v":"Watch TV"},{"v":2}]},{"c":[{"v":"Sleep"},{"v":7,"f":"7.000"}]}]}
        val = {"rows": [{"c": [{"v": None}, {"v": 0}, {"v": -1.0}, {"v": -1.0}, {"v": 0}, {"v": 0.10314003859558614}, {"v": 0.7826350260448144}, {"v": -0.8130331453280795}, {"v": 23200.0}, {"v": 199.0}, {"v": 13}, {"v": 200.0}, {"v": 203.0}, {"v": 9}, {"v": 0.1287553281467331}, {"v": -0.25764241296227725}, {"v": -3.9381700903769823}]}, {"c": [{"v": None}, {"v": 5}, {"v": 897.0}, {"v": 937.0}, {"v": 5}, {"v": 0.08665033676603795}, {"v": 0.7518657873829434}, {"v": -0.6491859913057388}, {"v": 23400.0}, {"v": 246.0}, {"v": 8}, {"v": 249.0}, {"v": 252.0}, {"v": 13}, {"v": 0.12467434512421184}, {"v": -0.30878588256514583}, {"v": -4.234706968036659}]}, {"c": [{"v": None}, {"v": 5}, {"v": 756.0}, {"v": 796.0}, {"v": 5}, {"v": 0.06917053429732452}, {"v": 0.7065398873108217}, {"v": -0.4568940724016388}, {"v": 23600.0}, {"v": 307.0}, {"v": 13}, {"v": 308.0}, {"v": 310.0}, {"v": 4}, {"v": 0.12166656748142592}, {"v": -0.3681170702764589}, {"v": -4.515692106261017}]}, {"c": [{"v": 668.0}, {"v": 8}, {"v": 643.0}, {"v": 651.0}, {"v": 15}, {"v": 0.15305605752615425}, {"v": 0.551878304534173}, {"v": -3.783418388352659}, {"v": 23800.0}, {"v": 370.0}, {"v": 13}, {"v": 377.0}, {"v": 380.0}, {"v": 4}, {"v": 0.11601208353248117}, {"v": -0.43234383475205207}, {"v": -4.626457472864576}]}, {"c": [{"v": 536.0}, {"v": 8}, {"v": 524.0}, {"v": 540.0}, {"v": 15}, {"v": 0.14488937693281984}, {"v": 0.496787463346073}, {"v": -3.6799128088230453}, {"v": 24000.0}, {"v": 454.0}, {"v": 13}, {"v": 459.0}, {"v": 463.0}, {"v": 4}, {"v": 0.11243933205263512}, {"v": -0.5040128056340836}, {"v": -4.709556903241067}]}, {"c": [{"v": 433.0}, {"v": 5}, {"v": 422.0}, {"v": 426.0}, {"v": 4}, {"v": 0.14133844893390238}, {"v": 0.4384634388814806}, {"v": -3.635465401601853}, {"v": 24200.0}, {"v": 545.0}, {"v": 8}, {"v": 554.0}, {"v": 559.0}, {"v": 4}, {"v": 0.10680389782080972}, {"v": -0.5813432653714156}, {"v": -4.609331667196781}]}, {"c": [{"v": 339.0}, {"v": 13}, {"v": 333.0}, {"v": 336.0}, {"v": 4}, {"v": 0.13701853367669384}, {"v": 0.3782401877904869}, {"v": -3.4689303296964726}, {"v": 24400.0}, {"v": 663.0}, {"v": 8}, {"v": 664.0}, {"v": 669.0}, {"v": 4}, {"v": 0.10412411926510369}, {"v": -0.6579560810486558}, {"v": -4.478018972534759}]}, {"c": [{"v": 263.0}, {"v": 13}, {"v": 258.0}, {"v": 261.0}, {"v": 9}, {"v": 0.13426029637185147}, {"v": 0.3198261486390377}, {"v": -3.2585790706722135}, {"v": 24600.0}, {"v": 808.0}, {"v": 5}, {"v": 770.0}, {"v": 810.0}, {"v": 5}, {"v": 0.1050354544615176}, {"v": -0.7238166080971229}, {"v": -4.352834698986921}]}, {"c": [{"v": 201.0}, {"v": 8}, {"v": 197.0}, {"v": 199.0}, {"v": 9}, {"v": 0.13217043393596928}, {"v": 0.2649097174462373}, {"v": -2.989063157888885}, {"v": 24800.0}, {"v": None}, {"v": 5}, {"v": 909.0}, {"v": 949.0}, {"v": 5}, {"v": 0.0}, {"v": 0.0}, {"v": 0.0}]}], "cols": [{"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}, {"type": "number", "id": "strike", "label": "strike"}, {"type": "number", "id": "last", "label": "last"}, {"type": "number", "id": "bidq", "label": "bidq"}, {"type": "number", "id": "bid", "label": "bid"}, {"type": "number", "id": "ask", "label": "ask"}, {"type": "number", "id": "askq", "label": "askq"}, {"type": "number", "id": "ivol", "label": "ivol"}, {"type": "number", "id": "delta", "label": "delta"}, {"type": "number", "id": "theta", "label": "theta"}]}
        
        server.send_message_to_all(self.encode_message('init_chart', val))

    
    # Called for every client disconnecting
    def client_left(self, client, server):
        del self.clients[client['id']]
        print("Client(%d) disconnected" % client['id'])
    
    
    # Called when a client sends a message
    def message_received(self, client, server, message):
        if len(message) > 200:
            message = message[:200]+'..'
        print("Client(%d) said: %s" % (client['id'], message))
    

def main():
    wsw = WebSocketServerWrapper('hello')    
    wsw.start()
    PORT=9001
    server = WebsocketServer(PORT)
    wsw.set_server(server)
    server.set_fn_new_client(wsw.new_client)
    server.set_fn_client_left(wsw.client_left)
    server.set_fn_message_received(wsw.message_received)
    server.run_forever()
    
    
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
    
