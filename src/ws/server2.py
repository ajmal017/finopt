from websocket_server import WebsocketServer
import threading, logging, time, traceback
import json

# https://github.com/Pithikos/python-websocket-server


class WebSocketServerWrapper(WebsocketServer, threading.Thread):
 
    
    
    def __init__(self, name, host, port):
        threading.Thread.__init__(self, name=name)
        WebsocketServer.__init__(self, port, host)
        self.set_fn_new_client(self.new_client)
        self.set_fn_client_left(self.client_left)
        self.set_fn_message_received(self.message_received)
        self.start()
        self.cli_requests = {}
        
    def run(self):
           
        self.i = 0
        def gen_msg(x):
            self.i += 1
            if self.i % 2:
                return self.encode_message('rs_order_status', {'text': 'order stat to %s: %s' % (x['id'], time.ctime())})
            else:
                return self.encode_message('rs_quote', {'text': 'quote px to %s: %s' % (x['id'], time.ctime())})
            
        while 1:
            time.sleep(1.5)
            #print 'sending stuff.. %s' % str(list(self.clients.iteritems()))
            map(lambda x: self.send_message(x, gen_msg(x)), self.clients)
            
            
    def encode_message(self, event, stuff):
        return json.dumps({'event': event, 'msg': stuff})

            
    def new_client(self, client, server):
        print("New client connected and was given id %d" % client['id'])
        #self.send_message_to_all("Hey all, a new client has joined us")
        self.send_message(client, self.encode_message('rs_accepted', {'handle': {'id': client['id'], 'address': client['address']}}))
    
    
    # Called for every client disconnecting
    def client_left(self, client, server):
        print client.keys()
        print("Client(%d) disconnected" % client['id'])
    
    
    # Called when a client sends a message
    def message_received(self, client, server, message):
        if len(message) > 200:
            message = message[:200]+'..'
        print("Client(%d) said: %s" % (client['id'], message))
        ed = json.loads(message)
        if ed('event') == 'request':
            self.handle_client_request(client, ed)
            
            
    #def handle_client_request(self, client, ):


# test this program with RestStream client found under ws/tests/client2.py

def main():
    wsw = WebSocketServerWrapper('hello', 'localhost', 9001)    
    wsw.run_forever()
    
    
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
    
