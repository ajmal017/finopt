# -*- coding: utf-8 -*-
import sys, traceback
import logging
import os
import ast
import urllib, urllib2, cookielib
import datetime
import re
import json
import cherrypy
import hashlib
import uuid
import redis
import json
import optcal
import ConfigParser
import portfolio
from comms.alert_bot import AlertHelper


class QServer(object):
    
    config = None
    r_conn = None
    
    
    def __init__(self, r_conn, config):
        super(QServer, self).__init__()
        QServer.r_conn = r_conn
        QServer.config = config
        #print QServer.r_conn
        

    
    
    @cherrypy.expose
    def index(self):
        
        s_line = 'welcome!'
        #r_host = cherrypy.request.app.config['redis']['redis.server']
        #r_port = cherrypy.request.app.config['redis']['redis.port']
        #r_db = cherrypy.request.app.config['redis']['redis.db']
        #r_sleep = cherrypy.request.app.config['redis']['redis.sleep']
    
        #rs = redis.Redis(r_host, r_port, r_db)
        rs = QServer.r_conn
        s_line = rs.info()
        html =''
        for k, v in cherrypy.request.app.config.iteritems():
            html = html + '<dt>%s</dt><dd>%s</dd>' % (k, v)
       
        impl_link = "<a href='./opt_implv'>options implied vol curves</a>"
        pos_link = "<a href=./ws_position_chart>Positions</a>" 
        bubble_link = "<a href=./port_bubble_chart>Risk Distributions</a>"
        return """<html><body><li>%s</li><li>%s</li><li>%s</li><br><dl>%s</dl></br>%s</body></html>""" % (bubble_link, impl_link, pos_link, html, s_line)
 
 
    @cherrypy.expose
    def opt_chains(self): 
        r_host = cherrypy.request.app.config['redis']['redis.server']
        r_port = cherrypy.request.app.config['redis']['redis.port']
        r_db = cherrypy.request.app.config['redis']['redis.db']
        r_sleep = cherrypy.request.app.config['redis']['redis.sleep']
        opt_chains = cherrypy.request.app.config['redis']['redis.datastore.key.option_chains']
        rs = redis.Redis(r_host, r_port, r_db)        
        opt_chain_tmpl = '%s%s/opt-chains-tmpl.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
 
        f = open(opt_chain_tmpl)
        html_tmpl = f.read()

        s_dict = rs.get(opt_chains)
        matrix = json.loads(s_dict)
        
        strike = matrix.keys()[0]
        print matrix
        num_months = len(matrix[strike])
        s = '["strike",'
#         for i in range(num_months):          
#             s = s + "'P-%s', 'C-%s', " % (matrix[strike].keys()[i], matrix[strike].keys()[i])
        s = s + '],'
        for month, strikes in sorted(matrix.iteritems()):
            l = ''
            for strike, cp in sorted(strikes.iteritems()):
                l = l + '[%s,%s,%s,%s,%s],' % (strike, cp['P']['0'], cp['P']['1'], cp['P']['2'], cp['P']['3'])
                
                                
            s = s + l + '],\n'
        
        print s
        html_tmpl = html_tmpl.replace('{{{data}}}', s)
        
        return html_tmpl 
 
    @cherrypy.expose
    def opt_implv(self):
        #r_host = cherrypy.request.app.config['redis']['redis.server']
        #r_port = cherrypy.request.app.config['redis']['redis.port']
        #r_db = cherrypy.request.app.config['redis']['redis.db']
        #r_sleep = cherrypy.request.app.config['redis']['redis.sleep']
        
        opt_implv = cherrypy.request.app.config['redis']['redis.datastore.key.option_implv']
        #rs = redis.Redis(r_host, r_port, r_db)
        
        rs = QServer.r_conn
                
        opt_implv_tmpl = '%s%s/opt-chains-tmpl.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(opt_implv_tmpl)
        html_tmpl = f.read()

        s_dict = rs.get(opt_implv)
        
        # sample value
        # {u'25400': {u'20150828': {u'P': [u'null', 1400.0], u'C': [0.21410911336791702, 29.0]}, u'20150929': {u'P': [u'null', u'null'], u'C': [0.1934532406732742, 170.0]}}, ...
        matrix = json.loads(s_dict)
        

        
        strike = matrix.keys()[0]
        print matrix
        num_months = len(matrix[strike])
        s = '["strike",'
        
        sorted_months = sorted(matrix[strike].keys())
        for i in range(num_months):          
            s = s + "'P-%s', 'C-%s', " % (sorted_months[i], sorted_months[i])
        s = s + '],'
        for strike, items in sorted(matrix.iteritems()):
            s = s + '[%s,' % str(strike)
            l = ''
            for month, cp in sorted(items.iteritems()):
                print month, cp
                l = l + ''.join('%s,%s,' % (cp['P'][0], cp['C'][0]))
                                
            s = s + l + '],\n'
        
        html_tmpl = html_tmpl.replace('{{{data}}}', s)



        s = '["strike",'
        for i in range(num_months):          
            s = s + "'P-%s', 'C-%s', " % (sorted_months[i], sorted_months[i])
        s = s + '],'
        for strike, items in sorted(matrix.iteritems()):
            s = s + '[%s,' % str(strike)
            l = ''
            for month, cp in sorted(items.iteritems()):
                l = l + ''.join('%s,%s,' % (cp['P'][1], cp['C'][1]))
                                
            s = s + l + '],\n'
        
        print 'sorted months' + sorted_months[0]
        html_tmpl = html_tmpl.replace('{{{dataPremium}}}', s)
        html_tmpl = html_tmpl.replace('{{{thisContractMonth}}}', sorted_months[0])
        

        
        return html_tmpl


    

    @cherrypy.expose
    def opt_implv_ex(self):
        #r_host = cherrypy.request.app.config['redis']['redis.server']
        #r_port = cherrypy.request.app.config['redis']['redis.port']
        #r_db = cherrypy.request.app.config['redis']['redis.db']
        #r_sleep = cherrypy.request.app.config['redis']['redis.sleep']
        
        opt_implv = cherrypy.request.app.config['redis']['redis.datastore.key.option_implv']
        #rs = redis.Redis(r_host, r_port, r_db)
        
        rs = QServer.r_conn
                
        opt_implv_tmpl = '%s%s/opt-chains-ex-tmpl.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(opt_implv_tmpl)
        html_tmpl = f.read()

        s_dict = rs.get(opt_implv)
        
        # sample value
        # {u'25400': {u'20150828': {u'P': [u'null', 1400.0], u'C': [0.21410911336791702, 29.0]}, u'20150929': {u'P': [u'null', u'null'], u'C': [0.1934532406732742, 170.0]}}, ...
        matrix = json.loads(s_dict)
        

        
        strike = matrix.keys()[0]
        print matrix
        num_months = len(matrix[strike])
        s = '["strike",'
        
        sorted_months = sorted(matrix[strike].keys())
        for i in range(num_months):          
            s = s + "'P-%s', 'C-%s', " % (sorted_months[i], sorted_months[i])
        s = s + '],'
        for strike, items in sorted(matrix.iteritems()):
            s = s + '[%s,' % str(strike)
            l = ''
            for month, cp in sorted(items.iteritems()):
                print month, cp
                l = l + ''.join('%s,%s,' % (cp['P'][0], cp['C'][0]))
                                
            s = s + l + '],\n'
        
        html_tmpl = html_tmpl.replace('{{{data}}}', s)



        s = '[{label:"strikes",type:"number"},'
        for i in range(num_months):          
            s = s + "{label: 'P-%s', type:'number'},\
                {label: 'Pb-%s', id:'i0', type:'number', role:'interval'},\
                {label: 'Pa-%s', id:'i0', type:'number', role:'interval'},\
                {label: 'C-%s', type:'number', },\
                {label: 'Cb-%s', id:'i0', type:'number', role:'interval'},\
                {label: 'Ca-%s', id:'i0',type:'number', role:'interval'},"\
                 % (sorted_months[i], sorted_months[i],sorted_months[i], sorted_months[i],sorted_months[i], sorted_months[i])
        s = s + '],'
        for strike, items in sorted(matrix.iteritems()):
            s = s + '[%s,' % str(strike)
            l = ''
            for month, cp in sorted(items.iteritems()):
                l = l + ''.join('%s,%s,%s,%s,%s,%s,' % (cp['P'][1], cp['P'][2],cp['P'][3],cp['C'][1],cp['C'][2],cp['C'][3]))
                                
            s = s + l + '],\n'
        
        
        html_tmpl = html_tmpl.replace('{{{dataPremium}}}', s)
        
        html_tmpl = html_tmpl.replace('{{{thisContractMonth}}}', sorted_months[0])
        

        
        return html_tmpl








    @cherrypy.expose
    def ws_cal_implvol(self, s, x, cp, ed, xd, r, d, v, p, out='iv'):
        try:
        #spot, strike, callput, evaldate, exdate, rate, div, vol, premium
            rs = optcal.cal_implvol(float(s), float(x), cp, ed, xd, float(r), float(d), float(v), float(p))
            return str(rs['imvol'])
        except:
            return

    @cherrypy.expose
    def ws_cal_option(self, s, x, cp, ed, xd, r, d, v, out='npv'):
        #spot, strike, callput, evaldate, exdate, rate, div, vol
        
        keys = ['npv', 'delta', 'gamma', 'theta', 'vega'];
        
        try:
            rs = optcal.cal_option(float(s), float(x), cp, ed, xd, float(r), float(d), float(v))
            
            if out == 'csv':
                logging.debug('ws_cal_option: ' + ','.join(str(rs[s]) for s in keys))
                return ','.join(str(rs[s]) for s in keys)
            elif out == 'json':
                return json.dumps(rs)
            else:
                return str(rs[out])  
        except:
            #exc_type, exc_value, exc_traceback = sys.exc_info()
            return traceback.format_exc()
            
    @cherrypy.expose
    def ws_get_hist_implv(self, dataAt):
    # given a date string YYMMDDHHMM, this routine returns an array of
    # implied vols arranged in a format like the below
    #      [["strike",'P-20150828', 'C-20150828', 'P-20150929', 'C-20150929', ],
    #                      [21800,0.29153118077,null,0.241032122988,null,],
    #                      [22000,0.284002011642,null,0.238145680311,null,],
    #                      [22200,0.270501965746,null,0.222647164832,null,]]   
        pass
    
    
    @cherrypy.expose
    def ws_market_data(self, r_ckey, fid):
        if str(fid).upper() == 'ALL':
            return QServer.r_conn.get(r_ckey)
        val = QServer.r_conn.get(r_ckey)
        if val is None:
            return 'invalid request. Check your input again!'
        dict = json.loads(QServer.r_conn.get(r_ckey))
        return str(dict[fid]) 
 
    @cherrypy.expose
    def ws_position_chart(self):
        p = portfolio.PortfolioManager(config)
        p.retrieve_position()
        opt_pos_chart_tmpl = '%s%s/opt-pos-chart-tmpl.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(opt_pos_chart_tmpl)
        html_tmpl = f.read()
        html_tmpl = html_tmpl.replace('{{{dataPCpos}}}', p.get_grouped_options_str_array())
        
        html_tmpl = html_tmpl.replace('{{{dataTablePos}}}', p.get_tbl_pos_csv())
        
        html_tmpl = html_tmpl.replace('{{{option_months}}}', ''.join(('%s, ' % m) for m in p.get_traded_months()))
        v = p.group_pos_by_right()
        html_tmpl = html_tmpl.replace('{{{PRvsCR}}}}', '%0.2f : %0.2f' % (v[0][1], v[1][1]))
        
        #print p.get_portfolio_summary()
        #html_tmpl = html_tmpl.replace('{{{pos_summary}}}', ''.join('<li>%s:   %s</li>' % (x[0],x[1]) for x in p.get_portfolio_summary() ))
        #print '\n'.join('%s:\t\t%s' % (k,v) for k,v in sorted(json.loads(DataMap.rs.get(port_key)).iteritems()))
        
        
        return html_tmpl
 
    @cherrypy.expose
    def ws_position_summary(self):
        p = portfolio.PortfolioManager(config)
        keys = [("delta_1percent","number"),("delta_all","number"),("delta_c","number"),("delta_p","number"),\
                ("theta_1percent","number"),("theta_all","number"),("theta_c","number"),("theta_p","number"),\
                ("unreal_pl","number"),("last_updated","string"),("status","string")]
        d = p.get_portfolio_summary()
        
        dict= {}
        dict['cols'] = [{'label': x[0], 'type': x[1]} for x in keys]
        dict['rows'] = [{'v': d[x[0]]} for x in keys]
        print json.dumps(dict)
        return json.dumps(dict)

    @cherrypy.expose
    def ws_recal_pos(self, force_refresh=False):
        p = portfolio.PortfolioManager(config)
        if force_refresh:
            p.retrieve_position()
        l_gmap = p.recal_port()
        print l_gmap
        return json.dumps(l_gmap)        
        


    @cherrypy.expose
    def ws_pos_csv(self):
        p = portfolio.PortfolioManager(config)
        p.retrieve_position()
        s = "%s" % p.get_tbl_pos_csv_old()
        #s = "%s" % p.get_tbl_pos_csv()
        #print s
        s = s.replace(',[', '').replace(']', '<br>')
        
        print s
        return s[1:len(s)-3]
    
    @cherrypy.expose
    def getSHquote(self, qs):

#http://api.money.126.net/data/feed/0000001,1399001,1399300        
#_ntes_quote_callback({"0000001":{"code": "0000001", "percent": 0.015468, "askvol1": 0, "askvol3": 0, "askvol2": 0, "askvol5": 0,
# "askvol4": 0, "price": 2972.57, "open": 2978.03, "bid5": 0, "bid4": 0, "bid3": 0, "bid2": 0, "bid1": 0, "high": 3014.41, "low": 2929.0, 
#"updown": 45.28, "type": "SH", "bidvol1": 0, "status": 0, "bidvol3": 0, "bidvol2": 0, "symbol": "000001", "update": "2015/08/27 12:43:00", 
#"bidvol5": 0, "bidvol4": 0, "volume": 19800251400, "ask5": 0, "ask4": 0, "ask1": 0, "name": "\u4e0a\u8bc1\u6307\u6570", "ask3": 0, "ask2": 0,
# "arrow": "\u2191", "time": "2015/08/27 12:42:57", "yestclose": 2927.29, "turnover": 204156106776} });        
        url = 'http://api.money.126.net/data/feed/%s?callback=ne3587367b7387dc' % qs
        print url
        pg = urllib2.urlopen(url.encode('utf-8'))    
        s = pg.read().replace('ne3587367b7387dc(', '')
        
        
        s = s[:len(s)-2]
        print s
        return s
    
    

    
    
    @cherrypy.expose
    def ws_port_summary(self):    
        
        rs = QServer.r_conn
        ps_key = cherrypy.request.app.config['redis']['redis.datastore.key.port_summary']
        s_portsum = rs.get(ps_key)
        #dict = json.loads(s_portsum)
        return s_portsum
    

    @cherrypy.expose
    def ws_port_items(self):    
        
        rs = QServer.r_conn
        key = cherrypy.request.app.config['redis']['redis.datastore.key.port_items']
        s_portitems = rs.get(key)
        #dict = json.loads(s_portsum)
        return s_portitems
    
    
    @cherrypy.expose
    def port_bubble_chart(self):
    
        s_data = self.ws_bubble_data()

        bubble_chart_tmpl = '%s%s/bubble-port.html' % (cherrypy.request.app.config['/']['tools.staticdir.root'], cherrypy.request.app.config['/static']['tools.staticdir.tmpl'])
        f = open(bubble_chart_tmpl)
        html_tmpl = f.read()
        html_tmpl = html_tmpl.replace('{{{bubble_data}}}', s_data)
        
        contract_month = eval(cherrypy.request.app.config['market']['option.underlying.month_price'])[0][0]
        html_tmpl = html_tmpl.replace('{{{FUT_CONTRACT}}}', 'HSI-%s-FUT-' % (contract_month))
        
        
        
        s_acctitems, last_updated, account_no = self.ws_acct_data()
        print s_acctitems, last_updated, account_no
        html_tmpl = html_tmpl.replace('{{{barAcct}}}', s_acctitems)
        html_tmpl = html_tmpl.replace('{{{account_no}}}', account_no)
        html_tmpl = html_tmpl.replace('{{{last_updated}}}', last_updated)
        
        
        
        return html_tmpl
       
    @cherrypy.expose
    def ws_bubble_data(self):
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
        s_portitems = self.ws_port_items() 
        
        litems = json.loads(s_portitems)
        
        # only interested in unrealized items, pos != 0 
        ldict = filter(lambda x: x['6002'] <> 0, litems)
        
        lcontract = map(lambda x: x['contract'], ldict)
        lpos_delta = map(lambda x: x['6005'], ldict)
        lstrike = map(lambda x: x['contract'].split('-')[2], ldict)
        ltheta = map(lambda x:  x['6006'], ldict)
        lupl = map(lambda x: x['6008'], ldict)
        
        
        
        
        colnames = "[['contract', 'strike', 'unreal PL', 'theta', 'delta'],"
        print '----------------------'
        s_data = colnames + ''.join('["%s",%s,%s,%s,%s],' % (lcontract[i], lstrike[i], lupl[i], ltheta[i], abs(lpos_delta[i])) for i in range(len(lcontract)))+ ']'
      
        return s_data
    
    
            
    
    
    @cherrypy.expose
    def ws_acct_data(self):
        rs = QServer.r_conn
        key = cherrypy.request.app.config['redis']['redis.datastore.key.acct_summary']
        s_acctitems = rs.get(key)
        dict = json.loads(s_acctitems)
        colnames = "[['Category', 'Value', { role: 'style' } ],"
        unwanted_cols = ['DayTradesRemaining','last_updated', 'AccountType']
        s_data = colnames + ''.join('["%s", %s, "%s"],' % (k, '%s'%(v[0]), '#3366CC' if float(v[0]) > 500000 else '#DC3912') if k not in unwanted_cols else '' for k, v in dict.iteritems()   )+ ']'
      
        return (s_data, dict['last_updated'], dict['AccountType'][2])
    
    
    
    @cherrypy.expose
    def ws_msg_bot(self, msg):
        a = AlertHelper(self.config)
        a.post_msg(msg)  
        
        
            
         
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
    
    logconfig = eval(config.get("opt_serve", "opt_serve.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(**logconfig)            
    host = config.get("redis", "redis.server").strip('"').strip("'")
    port = config.get("redis", "redis.port")
    db = config.get("redis", "redis.db")    
    r_conn = redis.Redis(host,port,db)
    
    cherrypy.quickstart(QServer(r_conn, config), '/', cfg_path[0])
    
   
