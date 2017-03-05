# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread
import ConfigParser
from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
from time import sleep
import time, datetime
import optcal
import opt_serve
import cherrypy
import redis
from helper_func.py import dict2str, str2dict
from options_data import ContractHelper 
import uuid            

# 
# IB tick types code reference
# https://www.interactivebrokers.com/en/software/api/api.htm
                    
                    
#
#
# the program starts by connecting to IB to retrieve the current positions of the account
# for each position line returned by IB API
# the program derive the instrument underlying 
# next it subscribes market data for the instrument and the underlying
# finally it stores all the information into a map and also into the redis datastore
#                     

class PortfolioManagerEx():
    
    config = {}
    con = None
    r_conn = None
    
    #20150827 modifed the port data structure
    # port = {'<conId>': {'undly:'id', 'instr_id': 'id', 'contract':<contract tuple>, 'pos': []}, 's_pos' = [<a list containing lines of positions separated by commas]}
    # port = {'1234': 
    #            {'undly':{'id': 444}}, 'contract': c, 'pos': []    },
    #         '333322':
    #            {'undly':{'id': 444}}, 'contract': c, 'pos': []    },
    #         's_pos' : [
    #
    #                         ["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],["HSI","OPT","20150828","C","22600",50.0,0.0000,0.0000,],["HSI","OPT","20150828","C","23000",50.0,-1.0000,1770.0000,],["HSI","OPT","20150828","C","23600",50.0,-2.0000,1470.0000,],["HSI","OPT","20150828","C","23800",50.0,-1.0000,920.0000,],["HSI","OPT","20150828","C","24000",50.0,-2.0000,1820.0000,],["HSI","OPT","20150828","C","24200",50.0,-1.0000,3120.0000,],["HSI","OPT","20150828","C","24800",50.0,-1.0000,220.0000,],["HSI","OPT","20150828","P","18000",50.0,-2.0000,1045.0000,],["HSI","OPT","20150828","P","18600",50.0,-1.0000,1120.0000,],["HSI","OPT","20150828","P","18800",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","19800",50.0,-1.0000,870.0000,],["HSI","OPT","20150828","P","20200",50.0,-1.0000,970.0000,],["HSI","OPT","20150828","P","20800",50.0,-2.0000,970.0000,],["HSI","OPT","20150828","P","21600",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","21800",50.0,-7.0000,1955.7143,],["HSI","OPT","20150828","P","23200",50.0,1.0000,25930.0000,],["HSI","OPT","20150929","C","24400",50.0,1.0000,24880.0000,],["HSI","OPT","20150929","P","21600",50.0,0.0000,0.0000,],["HSI","OPT","20150929","P","21800",50.0,2.0000,52713.3333,],["HSI","OPT","20150929","P","22600",50.0,3.0000,39763.3333,],["MHI","OPT","20150828","C","24400",10.0,-1.0000,2603.0000,],["MHI","OPT","20150828","P","20800",10.0,-1.0000,313.0000,],["MHI","OPT","20150828","P","21000",10.0,-1.0000,363.0000,],["MHI","OPT","20150828","P","23600",10.0,5.0000,4285.0000,],["MHI","OPT","20150929","C","24400",10.0,1.0000,4947.0000,],["MHI","OPT","20150929","P","21600",10.0,1.0000,12657.0000,],["MHI","OPT","20150929","P","22600",10.0,1.0000,9877.0000,],["MHI","OPT","20150929","P","23600",10.0,4.0000,7757.0000,],
    #                         [180.000000,-2.0,0],[186.000000,-1.0,0],[188.000000,-1.0,0],[198.000000,-1.0,0],[202.000000,-1.0,0],[208.000000,-2.2,0],[210.000000,-0.2,0],[216.000000,-0.8,0],[218.000000,-5.0,0],[226.000000,0,0.0],[226.000000,3.2,0],[230.000000,0,-1.0],[232.000000,1.0,0],[236.000000,0,-2.0],[236.000000,1.8,0],[238.000000,0,-1.0],[240.000000,0,-2.0],[242.000000,0,-1.0],[244.000000,0,1.0],[248.000000,0,-1.0],
    #                    ]
    #    
    #        }
    port = {}
    #port = []
    grouped_options = None
    download_state = 'undone'
    quit = False
    interested_types = ['OPT', 'FUT']
    EXCHANGE = 'HKFE'
    cal_greeks_settings = {}
    rs_port_keys = {}
    # undlys = [(con_id, contract),...]
    undlys = []
    curr_con_id = 0
    session_id = None
    
    def __init__(self, config):
        self.config = config
        config = ConfigParser.ConfigParser()
        config.read("config/app.cfg")
        host = config.get("market", "ib.gateway").strip('"').strip("'")
        port = int(config.get("market", "ib.port"))
        appid = int(config.get("market", "ib.appid.portfolio"))   
        
        r_host = config.get("redis", "redis.server").strip('"').strip("'")
        r_port = config.get("redis", "redis.port")
        r_db = config.get("redis", "redis.db")             
        
        
        self.r_conn = redis.Redis(r_host, r_port, r_db)
        
        self.con = ibConnection(host, port, appid)
        self.con.registerAll(self.on_ib_message)
        #self.con.registerAll(self.xyz)

        self.cal_greeks_settings['rate'] = config.get("market", "portfolio.cal.param.rate")
        self.cal_greeks_settings['div'] = config.get("market", "portfolio.cal.param.div")
        self.rs_port_keys['port_conid_set'] = config.get("redis", "redis.datastore.key.port_conid_set").strip('"').strip("'")
        
        
        self.session_id = self.gen_session_id()
        logging.info("********** Starting up portfolio-ex manager")
        logging.info("********** session id preix is [%s], this id is prepended to every key in the redis db" % self.session_id)
        logging.info("**********")
        
        logging.info("********** clear the redis datastore for all records that start with this session id [%s]" % self.session_id)
        self.clear_redis_portfolio()
        
    
    
    def clear_redis_portfolio(self):
        map(lambda x: (self.r_conn.delete(x)), self.r_conn.keys(pattern='%s*'%(self.session_id)))
    
    def gen_session_id(self):
        #return uuid.uuid4()
        return 'pt'
    
    def retrieve_position(self):
        self.connect()
        self.port['s_pos'] = []
        self.con.reqPositions()


    def start(self):
        self.retrieve_position()
        while self.download_state <> 'done':
            pass
            
        def endless_loop():
            while not self.quit:
                self.console()
                
            # unsubscribe instruments
            sys.exit(0)
        
         
        thread.start_new_thread(endless_loop(), )
                
    def stop(self):
        self.quit = True



    def console(self):
        #try:
        print "Available commands are: t - pos in csv, k - port key maps, r - group position by right, j - dump json string, q - terminate program"
        cmd = raw_input(">>")
        input = cmd.split(' ')
        if input[0]  == "q":
            self.quit = True
        elif input[0] == 't':
            print self.get_tbl_pos_csv()
        elif input[0] == 'k':
            print self.print_portfolio_key_maps()
        elif input[0] == 'r':
            print self.group_pos_by_right()
        elif input[0] == 'j':        
            print json.dumps(self.port)
        else:
            pass
        #except:
            
            #exc_type, exc_value, exc_traceback = sys.exc_info()
            #traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        
    def subscribe(self, contract):

            #self.con.reqPositions()
            #self.con.reqAccountSummary(100,'All', 'EquityWithLoanValue,NetLiquidation')
            #self.con.register(self.on_ib_message, 'UpdateAccountValue')
        id = -1
        if self.isContractSubscribed(self.curr_con_id) == False:
            #logging.debug( 'MarketDataManager:subscribe subscrbing to' + ContractHelper.printContract(contract))
            id = self.curr_con_id
            self.con.reqMktData(self.curr_con_id, contract, '', False)
            #self.r_conn.sadd(self.rs_port_keys['port_conid_set'], self.curr_con_id)
            #self.r_sadd(self.rs_port_keys['port_conid_set'], self.curr_con_id)
            #self.r_conn.set(self.curr_con_id, {})
            #self.r_set(self.curr_con_id, {})
            
            
            logging.debug( 'PortfolioManagerEx:subscribe >> Add record into redis set rs_port_keys["port_conid_set"]- >%s_%s, contract=>%s' %\
                                                         (self.session_id, str(self.curr_con_id), ContractHelper.printContract(contract)))
            self.curr_con_id = self.curr_con_id + 1
            
        else:
            logging.debug("PortfolioManagerEx: Contract already subscribed %s" % ContractHelper.printContract(contract))
        return id


    #the wrapper functions add a prefix session id to every key
    #
    def r_set(self, key, val):
        return self.r_conn.set("%s_%s" % (self.session_id, key), val)
        
    
    def r_get(self, key):
        return self.r_conn.get("%s_%s" % (self.session_id, key))
    
    
    def r_sadd(self, key, val):
        return self.r_conn.sadd("%s_%s" % (self.session_id, key), val)
    
    def r_sismember(self, set, val):
        return self.r_conn.sismember("%s_%s" % (self.session_id, set), val)

    def r_smembers(self, key):
        return self.r_conn.smembers("%s_%s" % (self.session_id, key))

    def isContractSubscribed(self, id):
        
        #logging.debug("PortfolioManagerEx: isContractSubscribed %s" % ('True' if self.r_conn.sismember(self.rs_port_keys['port_conid_set'], id) else 'False')) 
        logging.debug("PortfolioManagerEx: isContractSubscribed %s" % ('True' if self.r_sismember(self.rs_port_keys['port_conid_set'], id) else 'False'))
        return self.r_sismember(self.rs_port_keys['port_conid_set'], id)


    def link_undly_to_instrument(self, pos_contract):
        
        # determine the fut contract name
        # check the underlying id list
        # if the contract is not subscribed yet, subscribe for mkt data and assign a new con id
        # link the underlying id to the contract
        # 
        
        
        
        # transform the pos_contract to its underlying secType
        undly = Contract()
        undly.m_symbol = pos_contract.m_symbol
        undly.m_expiry = pos_contract.m_expiry
        undly.m_exchange = PortfolioManagerEx.EXCHANGE
        undly.m_secType = 'FUT'
        undly.m_right = ''
        undly.m_strike = 0
        undly.m_conId = ''
        
        def equals(u1, u2):
            return True if u1.m_exchange == u2.m_exchange and \
               u1.m_expiry == u2.m_expiry and \
               u1.m_symbol == u2.m_symbol else False
           
        logging.debug("PortfolioManagerEx: link_undly_to_instrument > transform deriv to its underling instrument %s" \
                      % ContractHelper.printContract(pos_contract)) 
        
        for u in self.undlys:
            if equals(u[1], undly):
                # return the undelying con_id
                return u[0]

        logging.debug("PortfolioManagerEx: underlying is not subscribed yet. calling self.subscribe %s" \
                      % ContractHelper.printContract(undly)) 
        # the undelying is not in undlys
        id  = self.subscribe(undly)
        logging.debug("PortfolioManagerEx: underlying tick id is %d, adding the entry into self.undlys list (id, undly)" % id)
        # remember this id
        self.undlys.append((id, undly))
                
        return id       
            
            
        
    def construct_port(self, pos_msg):

        
        def split_contract(contract):
        # port line for google api and cherrypy consumption 
        #
        # exch,type,contract_mth,right, strike,con_ration,pos,avgcost
        #             
            toks = ContractHelper.printContract(contract).split('-')
            s = ''
            slots = [0, 1, 4, 6]
            s = s + '%s,%s,%s' % (','.join(toks[i] for i in slots), toks[5].replace('.0', ''), '50.0' if toks[0][1:] == 'HSI' else '10.0')
            s = s.replace('[', '')
            return s
        
        
        
        
        self.port[pos_msg.contract.m_conId]={}
        self.port[pos_msg.contract.m_conId]['contract'] = ContractHelper.convert2Tuple(pos_msg.contract)
        s = split_contract(pos_msg.contract) + ",%0.4f,%0.4f" % (pos_msg.pos, pos_msg.avgCost)
        self.port['s_pos'].append(s)
        
        # pos keys: imvol,delta,gamma,theta,vega,npv,avgcost,pos,spotp        
        keys = ['imvol','delta','gamma','theta','vega','npv','spotp']
        for k in keys:
            self.port[pos_msg.contract.m_conId][k] = 0.0
        self.port[pos_msg.contract.m_conId]['avgcost'] = pos_msg.avgCost
        self.port[pos_msg.contract.m_conId]['pos'] = pos_msg.pos
        

        undlyid = self.link_undly_to_instrument(pos_msg.contract)
        tickid = self.subscribe(pos_msg.contract)
        logging.debug("construct_port - portfolio id %d, contract id %d, link underlying id %d" % (pos_msg.contract.m_conId, tickid, undlyid))
        self.port[pos_msg.contract.m_conId]['undly'] = undlyid
        self.port[pos_msg.contract.m_conId]['instr_id'] = tickid
        
        
        self.r_sadd(self.rs_port_keys['port_conid_set'], pos_msg.contract.m_conId)
        jstr = json.dumps(self.port[pos_msg.contract.m_conId])
        self.r_set(pos_msg.contract.m_conId, jstr) 
        logging.debug('construct_port: id->%d dump->%s' % (pos_msg.contract.m_conId, jstr))
        
        
        def add_item(k, v):            
            if not self.r_get(k):
                self.r_set(k, [v])
            else:
                l = eval(self.r_get(k))
                l.append(v)
                self.r_set(k, l)
        
        # create reverse lookup map: instr tick id -> pos_con_id
        add_item(tickid, pos_msg.contract.m_conId)        
        add_item(undlyid, tickid)
            
            
            

    

    def on_ib_message(self, msg):
        
        #print msg.typeName, msg
        
        if msg.typeName in "position":
            if self.download_state == 'undone':
                
                logging.debug("on_ib_message: %s %s %0.4f,%0.4f" %  (msg.account, ContractHelper.printContract(msg.contract), msg.pos, msg.avgCost))
                if msg.contract.m_secType in self.interested_types:
                    
                    
                    
                    self.construct_port(msg)


            
        if msg.typeName == 'positionEnd':
            self.download_state = "done"
            logging.debug("on_ib_message: complete downloading positions")
            #logging.info('-------')
            #logging.info(json.dumps(self.port))

        

        if msg.typeName == 'tickPrice':
            id = msg.field
            print 'tick change for %d' % id
            for u in self.undlys:
                if u[0] == id:
                    print 'recal all pos affected by %d' % u[0]
                    return 
        
            print 'recal pos for '
            # worker_recal([port_ids...], price)
    
    
    
            
    def group_pos_by_strike(self):


        # split into lines of position       
        m = map(lambda x: x.split(','), self.port['s_pos'])
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
        m = map(lambda x: x.split(','), self.port['s_pos'])
        q = map(lambda x: (x[3], float(x[5]) * float(x[6])/50), m)
        u = dict(set(map(lambda x:(x[0], 0.0), q)))
        print u, q
        for x in q:
            u[x[0]] += x[1] 
        return [(a,b) for a,b in u.iteritems()]
    
    def get_grouped_options_str_array(self):
        s = ''
        for e in sorted(self.grouped_options):
            s += "[%f,%s,%s]," % (float(e[0])/100.0, e[2] if e[1] == 'P' else 0, e[2] if e[1] == 'C' else 0)
        return s
    
    
    def print_portfolio_key_maps(self):
        print ("port id, instr id, undly id -> [link instr id]")
        for pos in self.r_smembers(self.rs_port_keys['port_conid_set']):
            js = json.loads(self.r_get(pos))
            
            
            print ('%s,%s -> %s,%s -> %s'%(pos, js['instr_id'], self.r_get(js['instr_id']),\
                                           js['undly'], \
                                           self.r_get(js['undly'])))
        
        
    
    #def cal_implvol(spot, strike, callput, evaldate, exdate, rate, div, vol, premium):
    
    def get_tbl_pos_csv(self, calGreeks=False):
        s_cols = [0,1,2,3,4]
        i_cols = [5,6,7]
        s = '["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],'
        
        for l in sorted(self.port['s_pos']):
            content = ''    
            toks= l.split(',')
 #           print toks
            for i in s_cols:
                content += '"%s",' % toks[i]
            for i in i_cols:
                content += '%s,' % toks[i]
            
                
            s += "[%s]," % content
        return s  
    
         

 
 
                               

    def connect(self):
        self.con.connect()

        
    def disconnect(self):

        self.con.disconnect()
        self.quit = True

if __name__ == '__main__':
           
    logging.basicConfig(filename = "log/port.log", filemode = 'w', 
                        level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')      
    
    config = ConfigParser.ConfigParser()
    config.read("config/app.cfg")
    p = PortfolioManagerEx(config)
    p.start()
    


    
#     print p.get_tbl_pos_csv()
#     print p.get_grouped_options_str_array()
#     print p.group_pos_by_right()
    


    # sample ouput    
# ["exch","type","contract_mth","right","strike","con_ration","pos","avgcost"],["HSI","OPT","20150828","C","22600",50.0,0.0000,0.0000,],["HSI","OPT","20150828","C","23000",50.0,-1.0000,1770.0000,],["HSI","OPT","20150828","C","23600",50.0,-2.0000,1470.0000,],["HSI","OPT","20150828","C","23800",50.0,-1.0000,920.0000,],["HSI","OPT","20150828","C","24000",50.0,-2.0000,1820.0000,],["HSI","OPT","20150828","C","24200",50.0,-1.0000,3120.0000,],["HSI","OPT","20150828","C","24800",50.0,-1.0000,220.0000,],["HSI","OPT","20150828","P","18000",50.0,-2.0000,1045.0000,],["HSI","OPT","20150828","P","18600",50.0,-1.0000,1120.0000,],["HSI","OPT","20150828","P","18800",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","19800",50.0,-1.0000,870.0000,],["HSI","OPT","20150828","P","20200",50.0,-1.0000,970.0000,],["HSI","OPT","20150828","P","20800",50.0,-2.0000,970.0000,],["HSI","OPT","20150828","P","21600",50.0,-1.0000,1570.0000,],["HSI","OPT","20150828","P","21800",50.0,-7.0000,1955.7143,],["HSI","OPT","20150828","P","23200",50.0,1.0000,25930.0000,],["HSI","OPT","20150929","C","24400",50.0,1.0000,24880.0000,],["HSI","OPT","20150929","P","21600",50.0,0.0000,0.0000,],["HSI","OPT","20150929","P","21800",50.0,2.0000,52713.3333,],["HSI","OPT","20150929","P","22600",50.0,3.0000,39763.3333,],["MHI","OPT","20150828","C","24400",10.0,-1.0000,2603.0000,],["MHI","OPT","20150828","P","20800",10.0,-1.0000,313.0000,],["MHI","OPT","20150828","P","21000",10.0,-1.0000,363.0000,],["MHI","OPT","20150828","P","23600",10.0,5.0000,4285.0000,],["MHI","OPT","20150929","C","24400",10.0,1.0000,4947.0000,],["MHI","OPT","20150929","P","21600",10.0,1.0000,12657.0000,],["MHI","OPT","20150929","P","22600",10.0,1.0000,9877.0000,],["MHI","OPT","20150929","P","23600",10.0,4.0000,7757.0000,],
# [180.000000,-2.0,0],[186.000000,-1.0,0],[188.000000,-1.0,0],[198.000000,-1.0,0],[202.000000,-1.0,0],[208.000000,-2.2,0],[210.000000,-0.2,0],[216.000000,-0.8,0],[218.000000,-5.0,0],[226.000000,0,0.0],[226.000000,3.2,0],[230.000000,0,-1.0],[232.000000,1.0,0],[236.000000,0,-2.0],[236.000000,1.8,0],[238.000000,0,-1.0],[240.000000,0,-2.0],[242.000000,0,-1.0],[244.000000,0,1.0],[248.000000,0,-1.0],
# {'P': 0.0, 'C': 0.0} [('P', 0.8), ('P', 0.0), ('C', -2.0), ('C', -2.0), ('P', -1.0), ('C', 0.2), ('P', -1.0), ('P', -0.2), ('C', 0.0), ('P', -0.2), ('C', -1.0), ('P', -1.0), ('C', -1.0), ('P', 1.0), ('P', 0.2), ('P', 2.0), ('C', 1.0), ('P', -2.0), ('P', -7.0), ('P', 1.0), ('P', -1.0), ('P', -1.0), ('P', 0.2), ('C', -1.0), ('P', 3.0), ('C', -1.0), ('C', -0.2), ('P', -2.0)]
# [('P', -8.200000000000001), ('C', -7.0)]