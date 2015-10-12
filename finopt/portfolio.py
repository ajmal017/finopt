# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread, threading
from threading import Lock
import ConfigParser
from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
from time import sleep
import time, datetime
import optcal
import opt_serve
import cherrypy
import redis
from comms.epc import EPCPub



#from options_data import ContractHelper
import options_data
# Tick Value      Description
# 5001            impl vol
# 5002            delta
# 5003            gamma
# 5004            theta
# 5005            vega
# 5006            premium

# 6001            avgCost
# 6002            pos
# 6003            totCost
# 6004            avgPx
# 6005            pos delta
# 6006            pos theta
# 6007            multiplier
# 6020            pos value impact +1% vol change
# 6021            pos value impact -1% vol change


# IB tick types code reference
# https://www.interactivebrokers.com/en/software/api/api.htm
                    

class PortfolioManager():
    
    config = {}
    con = None
    r_conn = None
    port = []
    grouped_options = None
    download_state = 'undone'
    quit = False
    interested_types = ['OPT', 'FUT']
    rs_port_keys = {}
    ib_port_msg = []
    tlock = None
    epc = None
    
    def __init__(self, config):
        self.config = config
#        config = ConfigParser.ConfigParser()
#        config.read("config/app.cfg")
        host = config.get("market", "ib.gateway").strip('"').strip("'")
        port = int(config.get("market", "ib.port"))
        appid = int(config.get("market", "ib.appid.portfolio"))   
        self.rs_port_keys['port_conid_set'] = config.get("redis", "redis.datastore.key.port_conid_set").strip('"').strip("'")
        self.rs_port_keys['port_prefix'] = config.get("redis", "redis.datastore.key.port_prefix").strip('"').strip("'")        
        self.rs_port_keys['port_summary'] = config.get("redis", "redis.datastore.key.port_summary").strip('"').strip("'")
        self.epc = eval(config.get("portfolio", "portfolio.epc").strip('"').strip("'"))
        # instantiate a epc object if the config says so
        if self.epc['stream_to_Kafka']:
            self.epc['epc'] = EPCPub(config) 
        
        r_host = config.get("redis", "redis.server").strip('"').strip("'")
        r_port = config.get("redis", "redis.port")
        r_db = config.get("redis", "redis.db")             
        
        self.r_conn = redis.Redis(r_host, r_port, r_db)
        
        self.con = ibConnection(host, port, appid)
        self.con.registerAll(self.on_ib_message)
        #self.con.registerAll(self.xyz)
        
        
        self.tlock = Lock()
    
    def retrieve_position(self):
        self.connect()
        
        # clear previous saved values
        self.port = []
        self.ib_port_msg = []
        self.clear_redis_portfolio()
        
        self.subscribe()
        while not self.quit:
            if self.download_state == 'done':
                self.quit = True
                
    def clear_redis_portfolio(self):
        l = map(lambda x: (self.r_conn.delete(x)), self.r_conn.keys(pattern='%s*' % (self.rs_port_keys['port_prefix'])))
        #self.r_set(self.rs_port_keys['port_summary'], '{"Portfolio Retrieval": "Calculation in progress...Try again later!" }')
        self.r_conn.set(self.rs_port_keys['port_summary'], '{"Portfolio Retrieval": "Calculation in progress...Try again later!" }')

        logging.debug('clear_redis_portfolio: num items cleared: %d'% len(l))
        
    
    def subscribe(self):

            self.con.reqPositions()
            #self.con.reqAccountSummary(100,'All', 'EquityWithLoanValue,NetLiquidation')

            #self.con.register(self.on_ib_message, 'UpdateAccountValue')

    def on_ib_message(self, msg):
        
        
        
        if msg.typeName in "position":
            if self.download_state == 'undone':
                logging.debug("%s" %  (options_data.ContractHelper.printContract(msg.contract)))
                if msg.contract.m_secType in self.interested_types:
                    logging.debug("PortfolioManager: getting position...%s" % msg)
                    self.ib_port_msg.append(msg)
                    #self.construct_port(msg)
            
        if msg.typeName == 'positionEnd':
            

            for pm in self.ib_port_msg:
                self.construct_port(pm)
                
            self.recal_port()
            self.group_pos_by_strike()
            logging.debug("PortfolioManager: Complete position download. Disconnecting...")
            self.disconnect()
            


            self.download_state = "done"
            
    def group_pos_by_strike(self):


        # split into lines of position       
        m = map(lambda x: x.split(','), self.port)
        # transform each line into two elements. first one is a key created 
        # by combining right and strike, the second is the product of 
        # position * conversion ratio (HSI=50, MSI=10)
        n = map(lambda x: (x[3] + x[4], float(x[5]) * float(x[6])/50), m)
        
        #p = dict(set(map(lambda x:(x[0], 0), n)))
        
        def sumByKey(f, n):
            # filter the list with only unique keys
            # transform the list into a dict
            p = dict(set(map(f, n)))
            # add the numbers together on key
            l =[]
            for x in n:
                p[x[0]] += x[1]
                
            return [(k[1:], k[0:1],v) for k,v in p.iteritems()]
         
         
           
        #print len(n),n
        # initialize a list of strikes and the position sum to 0
        # pass the list to sumByKey routine
        self.grouped_options = sumByKey(lambda x:(x[0], 0), n)
        #print len(l), sorted(l)
        
    def group_pos_by_right(self):        
    # group by put, call (summing up all contracts by right type,ignoring strikes)
        # split into lines of position       
        m = map(lambda x: x.split(','), self.port)
        q = map(lambda x: (x[3], float(x[5]) * float(x[6])/50), m)
        u = dict(set(map(lambda x:(x[0], 0.0), q)))
#        print u, q
        for x in q:
            u[x[0]] += x[1] 
        return [(a,b) for a,b in u.iteritems()]
    
    def get_grouped_options_str_array(self):
        s = ''
        for e in sorted(self.grouped_options):
            s += "[%f,%s,%s]," % (float(e[0])/100.0, e[2] if e[1] == 'P' else 0, e[2] if e[1] == 'C' else 0)
        return s
    
    
    def get_traded_months(self):
        
        li = []
        for l in self.port:
            toks=  l.split(',')
            li.append(toks[2])
        #print sorted(list(set(li)))
        return sorted(list(set(li)))
    
#     def get_tbl_pos_csv(self):
#         s_cols = [0,1,2,3,4]
#         i_cols = [5,6,7]
#         s = '["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],'
#         
#         for l in sorted(self.port):
#             content = ''    
#             toks= l.split(',')
#  #           print toks
#             for i in s_cols:
#                 content += '"%s",' % toks[i]
#             for i in i_cols:
#                 content += '%s,' % toks[i]
#             
#                 
#             s += "[%s]," % content
#         return s       


    def get_greeks(self, c_key):
        #print c_key, len(c_key), self.r_conn.get('a')
        
        gdata = self.r_conn.get(c_key)
        
        #print gdata
        gmap = json.loads(gdata) if gdata <> None else None
        return gmap


    def get_pos_contracts(self):
        s_cols = [0,2,4,3]
        cs = []
        for l in sorted(self.port):
            content = ''    
            toks= l.split(',')
            c_key = '-'.join('%s' % toks[i] for i in s_cols)
            cs.append(c_key)
        return cs

    def get_tbl_pos_csv(self):

        # WARNING: this routine will crash if a new instrument is traded during the day
        # but it is not previously been subscribed 
        # the gmap will be missing from redis
        # the workaround is to restart options_data.py 
        # to refresh all positions and add back any
        # new instruments to its subscription list 

        pall = set(self.r_conn.keys(pattern='%s*' % self.rs_port_keys['port_prefix']))
        s = '["symbol","right","avgcost","spotpx","pos","delta","theta","pos_delta","pos_theta","unreal_pl","last_updated"],'
        
        def split_toks(x):
            pmap = json.loads(self.r_conn.get(x))
            #print pmap
            gmap = json.loads(self.r_conn.get(x[3:]))
            #print gmap
            s = '["%s","%s",%f,%f,%f,%f,%f,%f,%f,%f,"%s"],' % (x[3:], x[len(x)-1:], pmap['6001'], gmap['5006'], pmap['6002'],\
                                                                         gmap['5002'],gmap['5004'],\
                                                                         pmap['6005'],pmap['6006'],pmap['6008'],pmap['last_updated'])
            return s                                                          
            
        end_s = s + ''.join (split_toks( x ) for x in pall)
        return end_s 


    def get_tbl_pos_csv_old(self, calGreeks=False):
        s_cols = [0,1,2,3,4]
        i_cols = [5,6,7]
        s = '["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],'
        
        for l in sorted(self.port):
            content = ''    
            toks= l.split(',')
 #           print toks
            for i in s_cols:
                content += '"%s",' % toks[i]
            for i in i_cols:
                content += '%s,' % toks[i]
            
                
            s += "[%s]," % content
        return s  

    def get_portfolio_summary(self):

        
        return json.loads(self.r_conn.get(self.rs_port_keys['port_summary']))
    

    def recal_port(self):
        # http://stackoverflow.com/questions/448034/multithreaded-resource-access-where-do-i-put-my-locks
        
        # a wrapper just to ensure only one thread is doing the portfolio recalculation
        logging.debug('recal_port: acquiring lock...%s' % threading.currentThread())
        self.tlock.acquire()
        try:
            s = self.recal_port_rentrant_unsafe()
        finally:
            logging.debug('recal_port: completed recal. releasing lock...')
            self.tlock.release()  
            
        return s

    def recal_port_rentrant_unsafe(self):
        
        
        logging.debug('PortfolioManager: recal_port')

        # retrieve the portfolio entries from redis, strip the prefix in front
        plines = map(lambda k: (k.replace(self.rs_port_keys['port_prefix'] + '_', '')),\
                                 self.r_conn.keys(pattern=('%s*'% self.rs_port_keys['port_prefix'])))
                                 
        logging.info ("PortfolioManager: recal_port-> gathering position entries from redis %s" % plines)
        
        pos_summary = {'delta_c': 0.0, 'delta_p': 0.0, 'delta_all': 0.0,\
                       'theta_c': 0.0, 'theta_p': 0.0, 'theta_all': 0.0,\
                       'delta_1percent' : 0.0, 'theta_1percent' : 0.0,\
                       'iv_plus1p': 0.0, 'iv_minus1p': 0.0, 'unreal_pl': 0.0}
        l_gmap = []
        l_skipped_pos =[]       
        t_pos_multiplier = 0.0
        
        for ckey in plines:
        
            gmap = self.get_greeks(ckey)
            logging.debug('PortfolioManager: recal_port greeks market data %s->%s ' % (ckey, gmap))
            logging.debug('PortfolioManager: recal_port position-map->%s' % (self.r_get(ckey)))
            pmap = json.loads(self.r_get(ckey)) 
            
            
            
            if gmap:
            
            # Tick Value      Description
            # 5001            impl vol
            # 5002            delta
            # 5003            gamma
            # 5004            theta
            # 5005            vega
            # 5006            premium        
            # 6001            avgCost
            # 6002            pos
            # 6003            totCost
            # 6004            avgPx
            # 6005            pos delta
            # 6006            pos theta
            # 6007            multiplier
            # 6009            curr_port_value
            # 6008            unreal_pl
            # 6020            pos value impact +1% vol change
            # 6021            pos value impact -1% vol change
            
           
                def pos_delta():                 
                    pd = pmap['6002'] * gmap['5002'] * pmap['6007']
                    logging.debug('PortfolioManager: recal_port: pos_delta: %f' % pd)
                    return pd
                
                def pos_theta():
                    pd = pmap['6002'] * gmap['5004'] * pmap['6007']
                    logging.debug('PortfolioManager: recal_port: pos_theta: %f' % pd)
                    return pd
 
                def pos_avg_px():
                    pd = pmap['6001'] / pmap['6007']
                    logging.debug('PortfolioManager: recal_port: pos_avg_px: %f' % pd)
                    return pd                   
                    
                def pos_tot_cost():
                    pd = pmap['6001'] * pmap['6002'] * pmap['6007']
                    logging.debug('PortfolioManager: recal_port: pos_tot_cost: %f' % pd)
                    return pd                   
                
                def pos_unreal_pl():
                    #(spot premium * multiplier - avgcost) * pos) 
                    v = (gmap['5006'] * pmap['6007'] - pmap['6001']) * pmap['6002'] 
                    return v 
    
                
    
                #logging.debug('PortfolioManager: recal_port greeks %f' % pos_delta(gmap))
                
                pmap['6005'] = pos_delta()
                pmap['6006'] = pos_theta()
                pmap['6004'] = pos_avg_px()
                pmap['6003'] = pos_tot_cost()
                pmap['6008'] = pos_unreal_pl()
                pmap['last_updated'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                
                l_gmap.append(pmap)          

                #t_pos_multiplier += (pmap['6002'] * pmap['6007'])

                right = ckey.split('-')[3].lower()
                pos_summary['delta_' + right] += pmap['6005']
                pos_summary['delta_all'] += pmap['6005']
                pos_summary['theta_' + right] += pmap['6006']
                pos_summary['theta_all'] += pmap['6006']
                pos_summary['unreal_pl'] += pmap['6008']

                
                
                #pos_summary['delta_1percent'] += (pos_summary['delta_all'] / (pmap['6002'] * pmap['6007']))
                # delta_1% = pos_ / (pos * multiplier)
                
                
                #print 'con,right,avgcost,spot px,pos,delta,theta, pos_delta,pos_theta,pos_unreal_pl,last_updated'
#                 print ( 'PT_entries: %s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%s' % (ckey, right, pmap['6001'], gmap['5006'], pmap['6002'],\
#                                                                      gmap['5002'],gmap['5004'],\
#                                                                      pmap['6005'],pmap['6006'],pmap['6008'],pmap['last_updated']
#                                                                      ))
#20150911                
                #print pmap
                #print json.dumps(pmap)
                self.r_set(ckey, json.dumps(pmap))
                logging.debug('PortfolioManager: update position in redis %s' % self.r_get(ckey))
                
            else:
                l_skipped_pos.append(ckey)

            
#            self.r_set(ckey, json.dumps(pmap))
#            logging.debug('PortfolioManager: update position in redis %s' % self.r_get(ckey))

 
        #pos_summary['delta_1percent'] = (pos_summary['delta_all'] / t_pos_multiplier)
        
        

 
        if len(l_skipped_pos) > 0:
            logging.warn('***************** PortfolioManager: recal_port. SOME POSITIONS WERE NOT PROCESSED!')
            logging.warn('----------------- DO NOT rely on the numbers in the Summary dictionary (pos_summary)')
            logging.warn('----------------- Please check your portfolio through other means or subscribe missing')
            logging.warn('----------------- market data in options_serve.py console. ')
        logging.info('-------------- POSITION SUMMARY')
        pos_summary['last_updated'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')    
        pos_summary['entries_skipped'] = l_skipped_pos
        pos_summary['status'] = 'OK' if len(l_skipped_pos) == 0 else 'NOT_OK'
        #self.r_set(self.rs_port_keys['port_summary'], json.dumps(pos_summary) )
        t_pos_summary = json.dumps(pos_summary)
        self.r_conn.set(self.rs_port_keys['port_summary'], t_pos_summary )
  
        #print pos_summary
        #print l_gmap      
        # broadcast 
        if self.epc['epc']:
            
            self.epc['epc'].post_portfolio_summary(pos_summary)
            self.epc['epc'].post_portfolio_items(l_gmap)
        

        #logging.info(pos_summary)
        
        logging.warn('-------------- Entries for which the greeks are not computed!! %s' %\
                        ','.join(' %s' % k for k in l_skipped_pos))
        
        return l_gmap
 

    def construct_port(self, pos_msg):
        # port structure
        #
        # exch,type,contract_mth,right, strike,con_ration,pos,avgcost
        # 
        # Tick Value      Description
        # 5001            impl vol
        # 5002            delta
        # 5003            gamma
        # 5004            theta
        # 5005            vega
        # 5005            premium
        
        # 6001            avgCost
        # 6002            pos
    
        
        toks = options_data.ContractHelper.printContract(pos_msg.contract).split('-')
        s = ''
        
        slots = [0, 1, 4, 6]
        s = s + '%s,%s,%s' % (','.join(toks[i] for i in slots), toks[5].replace('.0', ''), '50.0' if toks[0][1:] == 'HSI' else '10.0')
        s = s.replace('[', '') + ",%0.4f,%0.4f" % (pos_msg.pos, pos_msg.avgCost)
        
        
        self.port.append(s)
                
        ckey = options_data.ContractHelper.makeRedisKey(pos_msg.contract)
        multiplier = 50.0 if toks[0][1:] == 'HSI' else 10.0

        
        
        self.r_set(ckey, json.dumps({"contract": ckey, "6002": pos_msg.pos, "6001": pos_msg.avgCost, "6007": multiplier}))
        #self.recal_port(ckey)
 
    #the wrapper functions add a prefix session id to every key
    #
    def r_set(self, key, val):
        return self.r_conn.set("%s_%s" % (self.rs_port_keys['port_prefix'], key), val)
        
    
    def r_get(self, key):
        return self.r_conn.get("%s_%s" % (self.rs_port_keys['port_prefix'], key))
    
    
    def r_sadd(self, key, val):
        return self.r_conn.sadd("%s_%s" % (self.rs_port_keys['port_prefix'], key), val)
    
    def r_sismember(self, set, val):
        return self.r_conn.sismember("%s_%s" % (self.rs_port_keys['port_prefix'], set), val)

    def r_smembers(self, key):
        return self.r_conn.smembers("%s_%s" % (self.rs_port_keys['port_prefix'], key))
       

    def connect(self):
        self.con.connect()

        
    def disconnect(self):

        self.con.disconnect()
        self.quit = True

if __name__ == '__main__':
           
#     logging.basicConfig(#filename = "log/port.log", filemode = 'w', 
#                         level=logging.DEBUG,
#                         format='%(asctime)s %(levelname)-8s %(message)s')      
#     
#     config = ConfigParser.ConfigParser()
#     config.read("config/app.cfg")
#     p = PortfolioManager(config)
#     p.retrieve_position()
#     print p.get_portfolio_summary()
#     print p.get_tbl_pos_csv()

    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]    
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0:      
        raise ValueError, "Failed to open config file" 
    
    logconfig = eval(config.get("portfolio", "portfolio.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        

    p = PortfolioManager(config)
    p.retrieve_position()
    print p.get_portfolio_summary()
    print p.get_tbl_pos_csv()



    #p.recal_port()
#    print p.get_tbl_pos_csv()
#     print p.get_grouped_options_str_array()
#     print p.group_pos_by_right()

    # sample ouput    
# ["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],["HSI","OPT","20150828","C","22600",50.0,0.0000,0.0000,],["HSI","OPT","20150828","C","23000",50.0,-1.0000,1770.0000,],["HSI","OPT","20150828","C","23600",50.0,-2.0000,1470.0000,],["HSI","OPT","20150828","C","23800",50.0,-1.0000,920.0000,],["HSI","OPT","20150828","C","24000",50.0,-2.0000,1820.0000,],["HSI","OPT","20150828","C","24200",50.0,-1.0000,3120.0000,],["HSI","OPT","20150828","C","24800",50.0,-1.0000,220.0000,],["HSI","OPT","20150828","P","18000",50.0,-2.0000,1045.0000,],["HSI","OPT","20150828","P","18600",50.0,-1.0000,1120.0000,],["HSI","OPT","20150828","P","18800",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","19800",50.0,-1.0000,870.0000,],["HSI","OPT","20150828","P","20200",50.0,-1.0000,970.0000,],["HSI","OPT","20150828","P","20800",50.0,-2.0000,970.0000,],["HSI","OPT","20150828","P","21600",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","21800",50.0,-7.0000,1955.7143,],["HSI","OPT","20150828","P","23200",50.0,1.0000,25930.0000,],["HSI","OPT","20150929","C","24400",50.0,1.0000,24880.0000,],["HSI","OPT","20150929","P","21600",50.0,0.0000,0.0000,],["HSI","OPT","20150929","P","21800",50.0,2.0000,52713.3333,],["HSI","OPT","20150929","P","22600",50.0,3.0000,39763.3333,],["MHI","OPT","20150828","C","24400",10.0,-1.0000,2603.0000,],["MHI","OPT","20150828","P","20800",10.0,-1.0000,313.0000,],["MHI","OPT","20150828","P","21000",10.0,-1.0000,363.0000,],["MHI","OPT","20150828","P","23600",10.0,5.0000,4285.0000,],["MHI","OPT","20150929","C","24400",10.0,1.0000,4947.0000,],["MHI","OPT","20150929","P","21600",10.0,1.0000,12657.0000,],["MHI","OPT","20150929","P","22600",10.0,1.0000,9877.0000,],["MHI","OPT","20150929","P","23600",10.0,4.0000,7757.0000,],
# [180.000000,-2.0,0],[186.000000,-1.0,0],[188.000000,-1.0,0],[198.000000,-1.0,0],[202.000000,-1.0,0],[208.000000,-2.2,0],[210.000000,-0.2,0],[216.000000,-0.8,0],[218.000000,-5.0,0],[226.000000,0,0.0],[226.000000,3.2,0],[230.000000,0,-1.0],[232.000000,1.0,0],[236.000000,0,-2.0],[236.000000,1.8,0],[238.000000,0,-1.0],[240.000000,0,-2.0],[242.000000,0,-1.0],[244.000000,0,1.0],[248.000000,0,-1.0],
# {'P': 0.0, 'C': 0.0} [('P', 0.8), ('P', 0.0), ('C', -2.0), ('C', -2.0), ('P', -1.0), ('C', 0.2), ('P', -1.0), ('P', -0.2), ('C', 0.0), ('P', -0.2), ('C', -1.0), ('P', -1.0), ('C', -1.0), ('P', 1.0), ('P', 0.2), ('P', 2.0), ('C', 1.0), ('P', -2.0), ('P', -7.0), ('P', 1.0), ('P', -1.0), ('P', -1.0), ('P', 0.2), ('C', -1.0), ('P', 3.0), ('C', -1.0), ('C', -0.2), ('P', -2.0)]
# [('P', -8.200000000000001), ('C', -7.0)]

    
         
    

