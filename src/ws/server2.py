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
        print 'started...'
        while 1:
            time.sleep(1.5)
            #print 'sending stuff.. %s' % str(list(self.clients.iteritems()))
            map(lambda x: self.server.send_message(x[1], 'msg to %d: %s' % (x[0], time.ctime())), list(self.clients.iteritems()))
            
            

            
    def new_client(self, client, server):
        print("New client connected and was given id %d" % client['id'])
        self.clients[client['id']] = client
        server.send_message_to_all("Hey all, a new client has joined us")
    
    
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
    
