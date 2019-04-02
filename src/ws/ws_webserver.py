# -*- coding: utf-8 -*-
import sys, traceback
import logging
import cherrypy
import ConfigParser
import thread
import requests


class PortalServer(object):
    
    
    def __init__(self, config, ws_parent):
        super(PortalServer, self).__init__()
        PortalServer.config = config
        self.ws_parent = ws_parent
        
      

    
    
    @cherrypy.expose
    def index(self):
        
        return self.ws()
    
 
    @cherrypy.expose
    def expr(self):
        html = '%s%s/jtest2.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(html)
        return f.read()


    @cherrypy.expose
    def rest(self):
        url = 'http://localhost:6001/TDS'
        response = requests.get(url)
        return response.text
        
    
    @cherrypy.expose
    def ws(self):
        html = '%s%s/client_g.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(html)
        return f.read()
    
                 
class HTTPServe():
    
    def __init__(self, config, parent):
        self.config = config
        self.ws_parent = parent
        
    def start_server(self):
        cherrypy.quickstart(PortalServer(self.config, self.ws_parent), '/', self.config['ws_webserver_cfg_path'])
    
    def stop_server(self):
        cherrypy.engine.exit()   
                 
if __name__ == '__main__':
            
#     logging.basicConfig(filename = "log/opt.log", filemode = 'a', 
#                         level=logging.DEBUG,
#                         format='%(asctime)s %(levelname)-8s %(message)s')      
#  
# 
#     config = ConfigParser.ConfigParser()
#     config.read("config/app.cfg")
#     host = config.get("redis", "redis.server").strip('"').strip("'")
#     port = config.get("redis", "redis.port")
#     db = config.get("redis", "redis.db")    
#     r_conn = redis.Redis(host,port,db)
#     cherrypy.quickstart(QServer(r_conn, config), '/', "config/app.cfg")
   
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]    
    config = ConfigParser.ConfigParser()
    if len(config.read(cfg_path)) == 0:      
        raise ValueError, "Failed to open config file" 
    
    logconfig = eval(config.get("global", "logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(**logconfig)            


    cherrypy.quickstart(PortalServer(config, None), '/', cfg_path[0])
    
   
