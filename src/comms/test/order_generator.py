#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import json
import threading
import sys
from misc2.helpers import ContractHelper, ConfigMap, OrderHelper
from optparse import OptionParser
import requests
import traceback


order_function = {
                    'LMT': OrderHelper.limit_order,
                    'MKT': OrderHelper.market_order,
                    'LIT': OrderHelper.limit_if_touched,
                    'STP LMT': OrderHelper.stop_limit
                } 

def read_dat_v1(path, mode, url ):
    results = []
    f = open(path)
    lns = f.readlines()
    lns = filter(lambda l: l[0] <> '#' and l[0] <> '\n', lns)
    for l in lns:
        #print l
        CO =  l.split('|')
        C_tok = CO[0]
        O_tok = CO[1]
        
        vals = C_tok.split(':')
        contract = [None, None, None, None, None, None, None]
        read_vals = list(eval(vals[1]))
        contract = read_vals + contract[len(read_vals):]
        print contract
        c = ContractHelper.makeContract(tuple(contract))
        cs = ContractHelper.contract2kvstring(c)
        
        vals = O_tok.split(':')
            
        def format_val(v):
            try:           
                if '.' in v:        
                    return float(v)
                else:
                    return int(v)
            except ValueError:
                return v.replace("'", "").replace("\n","").replace(" ", "")
            
        toks = map(format_val, vals[1].split(','))
        f = order_function[toks[0]]
        args = toks[1:]
        order = f(*args)
        os =  OrderHelper.order2kvstring(order)
        if mode == 'sync':
            results.append(url % (cs, os))
        
    
    return results   


def format_contract_to_v2_str(contract):
    
    mmap = {"m_symbol": "symbol", "m_secType": "sec_type",  
     "m_right": "right", "m_expiry": "expiry", 
     "m_currency": "currency", "m_exchange": "exchange", "m_strike": 'strike'}
    
    cdict = {}
    for k,v in contract.__dict__.iteritems():
        if k in mmap and v <> None:
             cdict[mmap[k]] = v
    return json.dumps(cdict)

def format_order_to_v2_str(order):
    omap = {'m_orderType': 'order_type',
            'm_account': 'account', 'm_action': 'side',
            'm_totalQuantity': 'quantity', 
            'm_lmtPrice':  'price',
            'm_auxPrice': 'aux_price'
            }
    odict = {}
    for k,v in order.__dict__.iteritems():
        if k in omap and v <> None:
             odict[omap[k]] = v
    return json.dumps(odict)
    

def read_dat_v2(path, mode, url, version_digit ):
    results = []
    f = open(path)
    lns = f.readlines()
    lns = filter(lambda l: l[0] <> '#' and l[0] <> '\n', lns)
    for l in lns:
        #print l
        CO =  l.split('|')
        C_tok = CO[0]
        O_tok = CO[1]
        
        vals = C_tok.split(':')
        contract = [None, None, None, None, None, None, None]
        read_vals = list(eval(vals[1]))
        contract = read_vals + contract[len(read_vals):]
        c = ContractHelper.makeContract(tuple(contract))
        del c.__dict__['m_includeExpired']
        print c.__dict__
        
        cs = format_contract_to_v2_str(c)
        
        vals = O_tok.split(':')
            
        def format_val(v):
            try:           
                if '.' in v:        
                    return float(v)
                else:
                    return int(v)
            except ValueError:
                return v.replace("'", "").replace("\n","").replace(" ", "")
            
        toks = map(format_val, vals[1].split(','))
        f = order_function[toks[0]]
        args = toks[1:]
        order = f(*args)
        os =  format_order_to_v2_str(order)
        
        
        if mode == 'sync':
            results.append(url % (version_digit, cs, os))
        
    
    return results   



def api_post(url):

    try:
        
        response = requests.post(url) #, data=json.loads(name))
        return response.text
    except:
        return "invalid input. Check your json request string. %s" % traceback.format_exc()
            
    

def api_delete(url):
    try:
        
        response = requests.delete(url) #, data=json.loads(name))
        return response.text
    except:
        return "invalid input. Check your json request string. %s" % traceback.format_exc()

    
if __name__ == '__main__':

    kwargs = {
                'logconfig': {'level': logging.INFO},
                'mode': 'sync',
                'url': 'http://localhost:5001/v%s/order?contract=%s&order_condition=%s'
              }
    
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    
    parser.add_option("-f", "--dat_file",
                      action="store", dest="dat_file", 
                      help="path to the dat file")
    parser.add_option("-t", "--test_case",
                      action="store", dest="case_no", 
                      help="test case no: 1 = generate new order json only, 2= generate + send")
    parser.add_option("-v", "--version",
                      action="store", dest="version", 
                      )
    parser.add_option("-x", "--delete_id",
                      action="store", dest="del_ids", 
                      )
    
    
    (options, args) = parser.parse_args()
    if not options.case_no:
        parser.error('-t option not specified. specify -t 1 or -t 2. ')
    if not options.version:
        parser.error('-v version number either 1 or 2')

    
    for option, value in options.__dict__.iteritems():
    
        if value <> None:
            kwargs[option] = value

    
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        

    logging.info('config settings: %s' % kwargs)
    
    if kwargs['version'] == '1':
        results= read_dat_v1(kwargs['dat_file'], kwargs['mode'], kwargs['url'], kwargs['version'])
    else:
        results= read_dat_v2(kwargs['dat_file'], kwargs['mode'], kwargs['url'], kwargs['version'])
    if kwargs['case_no'] == '1':
        print '\n'.join(s for s in results)
    elif kwargs['case_no'] == '2':
        for r in results:
            print api_post(r)
    elif kwargs['case_no'] == '3':
        url = 'http://localhost:5001/v2/order?id=%s'
        ids = kwargs['del_ids'].split(',')
        map(lambda x:api_delete(url % x), ids)

