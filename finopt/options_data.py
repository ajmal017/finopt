# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import thread
import ConfigParser
from ib.ext.Contract import Contract
from ib.opt import ibConnection#, message
from time import sleep
import time, datetime
import optcal
import opt_serve
import cherrypy
import redis
import ystockquote
import portfolio
from misc2.helpers import ContractHelper

            
# Tick Value      Description
# 5001            impl vol
# 5002            delta
# 5003            gamma
# 5004            theta
# 5005            vega
# 5006            premium
# IB tick types code reference
# https://www.interactivebrokers.com/en/software/api/api.htm
class OptionsMarketDataManager():
    
    IMPL_VOL = 5001
    DELTA    = 5002
    GAMMA    = 5003
    THETA    = 5004
    VEGA     = 5005
    PREMIUM  = 5006
    

    
    # 
    # map to futures ticker ids key:contract_mth  val: tick_id undlys ={'20150828': 1} 
    undlys = {}
    

    # market data manager reference
    mdm = None
    rs = None
    config = {}
    
    cal_greeks_config = {}

    def __init__(self, config, mdm):
        
        self.mdm = mdm
        self.mdm.setOMDM(self)
        self.config = config

        # config file sample values        
        # option.underlying = "('HSI', 'FUT', 'HKFE', 'HKD', '', 0, '')"
        # option.underlying.month_price = "[['20150828', 22817.0], ['20150929', 22715.0]]"
        # option.underlying.tick_size = 200
        # option.chain_range = 0.08
        
        undly_tuple = eval(config.get("market", "option.underlying").strip('"').strip("'"))
        #undly_months_prices = eval(config.get("market", "option.underlying.month_price").strip('"').strip("'"))
        # 20160612
        undly_months_prices = DataMap().rskeys['option.underlying.month_price']
        
        undly_yahoo_ws = eval(config.get("market", "option.underlying.yahoo_ws").strip('"').strip("'"))
        self.cal_greeks_config = eval(config.get("market", "option.greeks.recal").strip('"').strip("'"))
    
        
        
        undly_tick_size = int(config.get("market", "option.underlying.tick_size"))
        undly_chain_range = float(config.get("market", "option.chain_range"))
    
        logging.debug("OptionsMarketDataManager --------------")
        logging.debug("OptionsMarketDataManager init: undly_tuple %s" % str(undly_tuple))
        logging.debug("OptionsMarketDataManager init: undly_months_prices %s" % str(undly_months_prices))
        logging.debug("OptionsMarketDataManager init: undly_tick_size %d" % undly_tick_size)
        logging.debug("OptionsMarketDataManager init: undly_chain_range %f" % undly_chain_range)
        logging.debug("OptionsMarketDataManager --------------")
        
        self.option_range(undly_tuple, undly_months_prices, undly_yahoo_ws, undly_tick_size, undly_chain_range)


    # 20150820 fix previous logic which onlyu support near month underlying subscription 
    #def option_range(self, contractTuple, months, undlypx, tsize, bound):
    def option_range(self, contractTuple, months_prices, undly_yahoo_ws, tsize, bound):
        

        for mth_price in months_prices:
            
            # set up the underlying structure
            # fix the contract month value
            undly = []
            for e in contractTuple:
                undly.append(e)
            # set month value
            undly[4] = mth_price[0] 


            # subscribe the underlying
            undly_tick_id = self.mdm.subscribe_tuple(tuple(undly))
            # store the tick id for lookup later
            self.undlys[mth_price[0]] = undly_tick_id
             
            logging.debug('OptionsMarketDataManager: option_range >>>>> subscribe underlying contract: %s' % undly)
            
            
            
            px = float(eval('%s("%s")' % (undly_yahoo_ws['func'], mth_price[2]))) if (undly_yahoo_ws['use_yahoo'] == True) else mth_price[1]
            logging.info("OptionsMarketDataManager: ********************")
            logging.warn("OptionsMarketDataManager: *** ")
            logging.info("OptionsMarketDataManager: ***")
            logging.info("OptionsMarketDataManager: *** Initial underlying price used: %.2f " % px)
            logging.info("OptionsMarketDataManager: *** Price obtained from %s->%s" % ("yahoo" if undly_yahoo_ws['use_yahoo'] else "config file", \
                                                                                       undly_yahoo_ws['func']))
            logging.info("OptionsMarketDataManager: ***")
            logging.info("OptionsMarketDataManager: ***")
            logging.info("OptionsMarketDataManager: ********************")            
            undlypx = round(px  / tsize) * tsize
            upper_limit = undlypx * (1 + bound)
            lower_limit = undlypx * (1 - bound)        
            
            
            logging.info("OptionsMarketDataManager: *** subscription boundaries between %0.4f and %0.4f" % (lower_limit, upper_limit))
            
            # send to IB conn to subscribe
            for i in range(int(undlypx), int(upper_limit ), tsize):
                self.add_subscription(contractTuple, undly[4], i, 'C')
                self.add_subscription(contractTuple, undly[4], i, 'P')
             
            for i in range(int(undlypx) - tsize, int(lower_limit), -tsize):
                self.add_subscription(contractTuple, undly[4], i, 'C')
                self.add_subscription(contractTuple, undly[4], i, 'P')            


    def add_subscription(self, contractTuple, month, X, right):
        
        #contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20150828', 0, '')
        opt = []
        for e in contractTuple:
            opt.append(e)
        opt[1] = 'OPT'
        opt[4] = month
        opt[5] = X if isinstance(X, int) else int(X)
        opt[6] = right
        id = self.mdm.subscribe_tuple(tuple(opt))
        logging.debug('OptionsMarketDataManager: add_subscription: %s, tick_id %d' % (opt, id))
        return id
        
    
    def determine_premium(self, msg):    


        premium = None
        
        
        #if msg.field == 4:
        if msg.field in [4,9]:
            premium = msg.price
            return premium
            
        if msg.field in [1,2]: #bid
            
            
                    
            if msg.price < 0:
                logging.debug("OptionsMarketDataManager determine_premium: received bogus price from IB feed. %0.4f" % msg.price)
                logging.debug("OptionsMarketDataManager determine_premium: attempt to use last price in datamap.")
                if 4 in DataMap().get(msg.tickerId):
                    last_price = DataMap().get(msg.tickerId)[4]
                    if last_price > 0:
                        premium = last_price
                        logging.debug("OptionsMarketDataManager determine_premium: using last price %0.4f" % premium)
                        return premium
                logging.debug("OptionsMarketDataManager determine_premium: No last price, no premium derived.")
                return None
        else:
            logging.debug("OptionsMarketDataManager determine_premium: price field not in 1,2 or 4. skip this message...")
            return None
        
        #   
        # at this point, msg price is > 0 and msg.field in 1 or 2
        #
        
        def derive_mid_price(field_id):
            bidask = 2 if (field_id == 1) else 1
	    logging.debug("********** dump %s" % str(DataMap().get(msg.tickerId)))
            if bidask in DataMap().get(msg.tickerId):
                if DataMap().get(msg.tickerId)[bidask] > 0.0:
                    mid = (DataMap().get(msg.tickerId)[bidask] + msg.price) / 2
                else:
                    mid = msg.price
            	logging.debug("OptionsMarketDataManager: incoming price field%d == %0.4f datamap field%d ==2 %0.4f"\
                           % (field_id, msg.price, bidask, DataMap().get(msg.tickerId)[bidask]))                                 
            return mid
        
        premium = derive_mid_price(msg.field)
        
                    
        if premium is None:
            # skip calculation because we don't have a valid value for premium 
            logging.debug("OptionsMarketDataManager: derive_mid_price: unable to derive a usable premium for ticker id %s, skipping computation" % (msg.tickerId))
            return None
        

                
            
            
    def on_tick_data_changed(self, msg):
        #print 'omdm %s' % msg
        if msg.typeName in ['tickPrice', 'tickSize']:
           
            if msg.typeName == 'tickPrice':
                
#                 if self.isTickIdAnOption(msg.tickerId):
#                 
#                     contract = DataMap().get(msg.tickerId)['contract']
#                     
#                     
#                     
#                     logging.info("OptionsMarketDataManager: on_tick_data_changed: received msg field %s for ticker id %s" % (msg.field, msg.tickerId))
#                     # last price%Y%m%d
#                     premium = None
#                     if msg.price < 0:
#                         logging.debug("OptionsMarketDataManager: received bogus price from IB feed. %0.4f" % msg.price) 
#                         return
#                     
#                     if msg.field == 4:
#                         premium = msg.price    
#                     elif msg.field == 1: #bid
#                         if 2 in DataMap().get(msg.tickerId):
#                             if DataMap().get(msg.tickerId)[2] > 0.0:
#                                 premium = (DataMap().get(msg.tickerId)[2] + msg.price) / 2
#                             else:
#                                 premium = msg.price
#                                 
#                             logging.debug("OptionsMarketDataManager: msgfiled ==1 %0.4f msgfiled ==2 %0.4f" % (msg.price, DataMap().get(msg.tickerId)[2]))                                 
#                     elif msg.field == 2: #ask
#                         if 1 in DataMap().get(msg.tickerId):
#                             if DataMap().get(msg.tickerId)[1] > 0.0:
#                                 premium = (DataMap().get(msg.tickerId)[1] + msg.price) / 2
#                             else:
#                                 premium = msg.price
#                             logging.debug("OptionsMarketDataManager: msgfiled ==2 %0.4f msgfiled ==1 %0.4f" % (msg.price, DataMap().get(msg.tickerId)[1]))                                
#                     if premium is None:
#                         # skip calculation because we don't have a valid value for premium 
#                         logging.debug("OptionsMarketDataManager: on_tick_data_changed: unable to derive a usable premium for ticker id %s, skipping computation" % (msg.tickerId))
#                         return
                if self.isTickIdAnOption(msg.tickerId):
                
                    contract = DataMap().get(msg.tickerId)['contract']
                    logging.info("OptionsMarketDataManager: on_tick_data_changed: received msg field %s for ticker id %s" % (msg.field, msg.tickerId))                    
                            
                    premium = self.determine_premium(msg)
                    if premium == None:
                        return
                    
                    
                    
                    undly = DataMap().get(self.getUndlyId(contract.m_expiry))
                    if 4 in undly:  # the last price
                        spot = undly[4]
                        
                        logging.info('OptionsMarketDataManager:on_tick_data_changed: undelying spot %0.4f of month %s' % (spot, contract.m_expiry))
                        today = time.strftime('%Y%m%d') 
                        logging.info('OptionsMarketDataManager:on_tick_data_changed: today %s ' % time.strftime('%Y%m%d'))
                        div = self.cal_greeks_config['div']
                        rate = self.cal_greeks_config['rate']
                        
                        # vol is not used in the calculation of implv but quantlib requires the parameter to be passed
                        vol = self.cal_greeks_config['vol']
                        logging.info('OptionsMarketDataManager:on_tick_data_changed: symbol %s, spot %s, X %s, right: %s, evaldate: %s, expiry: %s, rate: %0.4f, div: %0.4f, vol: %0.4f, premium: %0.4f' % 
                                        (contract.m_symbol, spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, vol, premium))
                        
                        try:
                        
                            iv = optcal.cal_implvol(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, vol, premium)
                            
                        except Exception, err:
                            logging.error(traceback.format_exc())
                            logging.error("OptionsMarketDataManager: *******************recovering from a implvol error ******")
                            
                            intrinsic = abs(contract.m_strike - spot)  
                            iv = optcal.cal_implvol(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, vol, premium)
                            logging.error("OptionsMarketDataManager: ******** Using intrinsic value to calculate premium %0.4f instead of the spot premium %0.4f"\
                                                                        % (intrinsic, premium ))
                        
                        logging.info('OptionsMarketDataManager:on_tick_data_changed: implied vol: %0.4f' % iv['imvol'])
                        results = optcal.cal_option(spot, contract.m_strike, contract.m_right, today, contract.m_expiry, rate, div, iv['imvol'])
                        
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.IMPL_VOL] = iv['imvol']
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.DELTA] = results['delta']
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.GAMMA] = results['gamma']
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.THETA] = results['theta']
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.VEGA] = results['vega']
                        DataMap().get(msg.tickerId)[OptionsMarketDataManager.PREMIUM] = results['npv'] 
        
                        # update Redis store
                        #
                        DataMap().update_rd(msg.tickerId)
        
                else: # underlying price changed
                    # check whether new option chains need to be added
                    #
                    # check logic to be implemented
                    
                    
                    # save undly tick to REDIS
                    contract = DataMap().get(msg.tickerId)['contract']
                    logging.debug('OptionsMarketDataManager:on_tick_data_changed: ------- Underlying Price updated: tick id %d datamap contract %s' % (msg.tickerId, ContractHelper.printContract(contract)))
                    
                    
                    DataMap().update_rd(msg.tickerId)
          
    
    # return tick_id of underlying 
    # this function has problem, it doesn't cater for the symbol type
    # so MHI contracts will be processed just fine but with HSI futures  
    # returned as its underlying
    # >>> ok as it doesn't casuse any problem in calculating the greeks
    # >>> but need to be fixed later
    def getUndlyId(self, contract_mth):
        return self.undlys[contract_mth]
    
    def isTickIdAnOption(self, tickid):
        return tickid not in self.undlys.values()
            

class MarketDataManager():
    
    config = {}
    tick_id = 0
    instrus = {}
    con = None
    omdm = None
    
    def __init__(self, config):
        self.config = config
        ibgw = config.get("market", "ib.gateway").strip('"').strip("'")
        ibport = config.get("market", "ib.port")
        ibid = config.get("market", "ib.appid")
        logging.debug("ibgw, port, app id: %s %s %s" % (ibgw, ibport, ibid))
        self.con = ibConnection(ibgw, int(ibport), int(ibid))
        
        self.con.registerAll(self.on_ib_message)

    

    def setOMDM(self, omdm):
        self.omdm = omdm
        self.omdm.mdm = self
        
    def subscribe_tuple(self, tuple):        
        return self.subscribe(ContractHelper.makeContract(tuple))

    def subscribe(self, contract):


        tick_id = -1
        if self.isContractSubscribed(contract) == False:
            #logging.debug( 'MarketDataManager:subscribe subscrbing to' + ContractHelper.printContract(contract))
            
            self.con.reqMktData(self.tick_id, contract, '', False)
            tick_id = self.tick_id
            #self.instrus[self.tick_id] = {}
            #self.instrus[self.tick_id]['contract'] = contract
            
            DataMap().set(self.tick_id, {})
            DataMap().get(self.tick_id)['contract'] = contract
            
            logging.debug( 'MarketDataManager:subscribe DataMap stored value - >' + ContractHelper.printContract(DataMap().get(self.tick_id)['contract']))
            self.tick_id = self.tick_id + 1
            
        else:
            logging.debug("Contract has been subscribed already %s" % ContractHelper.printContract(contract))
        return tick_id

    def isContractSubscribed(self, contract):
        
        
        
        #for c in self.instrus.values():
        # 20150814 - changed to rely on DataMap values
        for c in DataMap().getData().values():
            if c['contract'] == contract:
                logging.debug("MarketDataManager: isContractSubscribed: YES %s" % ContractHelper.printContract(c['contract']))
                return True   
        
        logging.debug("MarketDataManager: isContractSubscribed: No. Subscribing to %s" % ContractHelper.printContract(contract))
        return False


        

    def on_ib_message(self, msg):
        
        if msg.typeName in ['tickPrice', 'tickSize']:

#             if msg.tickerId not in self.instrus:
#                 self.instrus[msg.tickerId] = {}
            
            if msg.typeName == 'tickPrice':
                #self.instrus[msg.tickerId][msg.field] = msg.price
                DataMap().get(msg.tickerId)[msg.field] = msg.price
                 
            if msg.typeName == 'tickSize':
                #self.instrus[msg.tickerId][msg.field] = msg.size
                DataMap().get(msg.tickerId)[msg.field] = msg.size
            
            #if msg.typeName == "tickSize":
            #    logging.debug(msg)
            
            
        if self.omdm != None:
            self.omdm.on_tick_data_changed(msg)


                               

    def connect(self):
        rc = self.con.connect()
        logging.info("-----------------")
        logging.info("-----------MarketDataManager ----Connect to IB connection: status: %s" % str(rc))
        logging.info("-----------------")
        if rc == False:
            logging.error("-----------MarketDataManager ---- Connect to IB Failed!!! Terminating....")
            sys.exit(-1)
        
        
    def disconnect(self):
        for i in range(self.tick_id):
            self.con.cancelMktData(i)
            logging.debug("cancelling tick id subscription %d" % i)
        self.con.disconnect()

      
def test():
#     contractTuple = ('USD', 'CASH', 'IDEALPRO', 'JPY', '', 0.0, '')
#     contractTuple2 = ('xSD', 'CASH', 'IDEALPRO', 'JPY', '', 0.0, '')
#      
#     print 1 if contractTuple == contractTuple2 else -1
#     
#     
#     
#     contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20150929', 0, '')
#     
#     
#     d = DataMap()
#     d.set('c', ContractHelper.makeContract(contractTuple))
#     c = d.get('c')
#     print ContractHelper.printContract(c)
#     mdm = MarketDataManager({})
#     
#     o = OptionsMarketDataManager(mdm, c)
    
    def f1(*a):
        s = ''
        for i in a:
            s = s + '%s,' % i
        return s

    rs = redis.Redis()
    start = 201508200930
    end = int(datetime.datetime.now().strftime('%Y%m%d%H%M'))
    
    for i in range(start, end):
        if rs.exists(i):
            j= json.loads(rs.get(i))
            
            i = str(i)
# 
#             print '[new Date(%s,%s,%s,%s,%s), %s, %s, %s, undefined],' % (i[0:4], i[4:6], i[6:8], i[8:10], i[10:12],
#                                                         j['22400']['20150828']['P'][0], 
#                                                         j['22600']['20150828']['P'][0],
#                                                          j['22600']['20150929']['P'][0])
            s =  '[new Date(%s,%d,%s,%s,%s),' % (i[0:4], int(i[4:6])-1, i[6:8], i[8:10], i[10:12])
            s = s + f1(j['21400']['20150828']['P'][0], j['21800']['20150828']['P'][0], j['21800']['20150929']['P'][0], 'undefined', ']')
            print s

    

    sys.exit(-1)

class DataMap():
    instrus = {}
    rs = None
    rskeys = {}
    mkt_sessions = {}
    
    config = None


    def init_redis(self, config):
        #self.config = config
        DataMap.config = config
        host = config.get("redis", "redis.server").strip('"').strip("'")
        port = config.get("redis", "redis.port")
        db = config.get("redis", "redis.db")
        self.rskeys['redis.datastore.key.option_implv'] = config.get("redis", "redis.datastore.key.option_implv").strip('"').strip("'")
        self.rskeys['redis.datastore.key.option_chains'] = config.get("redis", "redis.datastore.key.option_chains").strip('"').strip("'")
        self.rskeys['redis.datastore.key.option_set'] = config.get("redis", "redis.datastore.key.option_set").strip('"').strip("'")
        self.rskeys['redis.datastore.key.option_implv_ts_set'] = config.get("redis", "redis.datastore.key.option_implv_ts_set").strip('"').strip("'")
        self.rskeys['redis.datastore.key.option_implv_ts'] = config.get("redis", "redis.datastore.key.option_implv_ts").strip('"').strip("'")
        
        
        self.rskeys['hkex.openhours'] = config.get("market", "hkex.openhours").strip('"').strip("'")
        self.rskeys['option.bid_ask_spread_tolerance'] = config.get("market", "option.bid_ask_spread_tolerance")
        
        DataMap.mkt_sessions = json.loads(self.rskeys['hkex.openhours'])
        

        
        DataMap.rs = redis.Redis(host, port, db)
        logging.info(self.rs.info())
        
        
        self.rskeys['option.underlying.month_price'] = self.set_option_calendar()


    # 20160612
    # routine to determine the near and far month contract expiry date
    #
    # logic:
    #      - determine today's date
    #
    #      - auto mode:
    #      - look up entries in redis
    #      - if records are found, use the records in redis
    #         else 
    #            retrive record from hkgov website
    #            update redis db
    #      -  
    # 
    # for all hard coded options end dates:
    # https://www.hkex.com.hk/eng/prod/drprod/hkifo/tradcalend_2.htm to get all dates 
    # 
    #
    def set_option_calendar(self):
        year = int(datetime.datetime.now().strftime('%Y'))
        month = int(datetime.datetime.now().strftime('%m'))


        holiday_key_prefix = config.get("redis", "redis.datastore.key.hkex_holiday_prefix").strip('"').strip("'")        
        
        rs = self.redisConn()
        holiday_key = '%s%s' % (holiday_key_prefix, year)
        holidays = rs.get(holiday_key)
                    
#        if holidays == None:
#	fix 2017-01-03
        if holidays == 'null':
            holidays = optcal.get_hk_holidays(year)
            logging.info("options_data:set_option_calendar: retrieved from gov.hk --> update Redis: [%s]: %s", holiday_key, json.dumps(holidays))
            rs.set(holiday_key, json.dumps(holidays))    

        else:
            holidays = json.loads(holidays)
            logging.info("options_data:set_option_calendar: retrieved holidays from redis %s", str(holidays))

        
        
        undly_months_prices = eval(config.get("market", "option.underlying.month_price").strip('"').strip("'"))
        
        undly_months_prices[0][0] = optcal.get_HSI_last_trading_day(holidays, month, year)
	next_month = (month + 1) % 12 if (month + 1) % 12 <> 0 else (month + 1) % 12 + 12
#2016/12 lc - fix year end bug 
	year = year if next_month <> 1 else year + 1
	
        logging.info("options_data:set_option_calendar: next month: year %s:%s " % (next_month, year))
        undly_months_prices[1][0] = optcal.get_HSI_last_trading_day(holidays, next_month, year)
        
        logging.info("options_data:set_option_calendar:  %s " % str(undly_months_prices))
        return undly_months_prices
    
    def redisConn(self):
        return DataMap.rs
    
    
    # this routine clones an element in instrus but without the contract object  
    def simple_clone(self, contract):
        instru = {}
        for key, stuff in contract.iteritems():
            if key <> 'contract':
                instru[key] = stuff

        return instru
        
        
    def update_rd(self, tickerId):
        
        # this routine saves options data into redis map
        # map structure
        # 
        #key:
        #    underlying
        #         month
        #            strike
        #                right
        #value:
        #                values
        rs = DataMap().redisConn()
        if rs == None:
            logging.error("update_rd: Redis connection is None! Has it been initialized?")
            return
    
        contract = DataMap().get(tickerId)['contract']
        r_key = ContractHelper.makeRedisKey(contract)
        
        rs.sadd(self.rskeys['redis.datastore.key.option_set'], r_key)
        data = DataMap().simple_clone(DataMap().get(tickerId))
        data['last_updated'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        
        jd = json.dumps(data)
        rs.set(r_key, jd)

        #logging.debug( "update rd: [%s] %s " % (r_key, jd)) 
        
    
    
    def get(self, key):
        return DataMap.instrus[key]
    
    def set(self, key, val):
        DataMap.instrus[key] = val
        
    
    def getData(self):
        return DataMap.instrus
    
    
    def isKeyInMap(self, k):
        return (k in DataMap.instrus)
    
    
    def createOptionsPanelData(self):

        # this function dumps the data in DataMap and turn it into a json string
        # set the value on redis map
        # and make it available for the web service to query the json string
        # it is up to the receiving end to format the json sring into whatever 
        # structure for viewing. for example, into a data array accepted by google charts
      
        matrix = {}
        for tick_id, instr_v in DataMap().getData().iteritems():
#20150910 - data now contains MHI 
# need to add extra condition to filter out MHI

            #if DataMap().get(tick_id)['contract'].m_secType == 'OPT':
            if DataMap().get(tick_id)['contract'].m_secType == 'OPT' and \
                DataMap().get(tick_id)['contract'].m_symbol == 'HSI':
                strike = DataMap().get(tick_id)['contract'].m_strike
                expiry = DataMap().get(tick_id)['contract'].m_expiry
                right = DataMap().get(tick_id)['contract'].m_right
                if expiry not in matrix:
                    matrix[expiry] = {}
                if strike not in matrix[expiry]:
                    matrix[expiry][strike] = {}
                if right not in matrix[expiry][strike]:
                    matrix[expiry][strike][right] = {}

                for field, value in sorted(instr_v.iteritems()):
                    if type(value) is Contract:
                        pass
                        #s = s + "%s," % ContractHelper.printContract(value)
                    else:
                        matrix[expiry][strike][right][field] = value
                     
                    
        
        
        DataMap.rs.set(DataMap.rskeys['redis.datastore.key.option_chains'], json.dumps(matrix))
        logging.debug("createOptionsPanelData: %s" % str( matrix))
    
    def printMap(self):

        for tick_id, instr_v in DataMap().getData().iteritems():
            s = 'Tick:%s [%s]>>' % (tick_id, ContractHelper.printContract(DataMap().get(tick_id)['contract'])) 
            for field, value in sorted(instr_v.iteritems()):
                if type(value) is Contract:
                    continue
                    #s = s + "%s," % ContractHelper.printContract(value)
                else:
                    s = s + "%d:[%s], " % (field, value)
                 
            print "%s" % s

    def printMapCSV(self):
        keys = [0,1,2,3,4,5,6,7,8,9,14,5001,5002,5003,5004,5005,5006]
        print 'tick id,contract,' + ','.join(str(k) for k in keys)
        lines = ''
        for tick_id, instr_v in DataMap().getData().iteritems():
            
            #s = '%s,%s-%s-%s,' % (tick_id, DataMap().get(tick_id)['contract'].m_right, DataMap().get(tick_id)['contract'].m_expiry, DataMap().get(tick_id)['contract'].m_strike)
            s = '%s,%s,' % (tick_id, ContractHelper.makeRedisKey(DataMap().get(tick_id)['contract']))
            for k in keys:
                if k in instr_v.keys():
                    s = s + "%s, " % (instr_v[k])
                else:
                    s = s + ','
            
            print s

            lines += "%s\n" % s
        return lines
        
        
    def createImplVolChartData(self):    

        
        matrix = {}
        for tick_id, instr_v in DataMap().getData().iteritems():
#            if DataMap().get(tick_id)['contract'].m_secType == 'OPT':
            if DataMap().get(tick_id)['contract'].m_secType == 'OPT' and \
                DataMap().get(tick_id)['contract'].m_symbol == 'HSI':
                
                strike = DataMap().get(tick_id)['contract'].m_strike
                expiry = DataMap().get(tick_id)['contract'].m_expiry
                right = DataMap().get(tick_id)['contract'].m_right
                if strike not in matrix:
                    matrix[strike] = {}
                if expiry not in matrix[strike]:
                    matrix[strike][expiry] = {}
                if right not in matrix[strike][expiry]:
#                    matrix[strike][expiry][right] = ['null','null']
#20150915                     
                    matrix[strike][expiry][right] = ['null','null','null','null']
                logging.debug('-------------########################------------------- instr_v.keys')
                logging.debug(instr_v.keys())
                if 5001 in instr_v.keys(): 
                    
                    matrix[strike][expiry][right][0] =  (instr_v[5001]) if self.isfloat(instr_v[5001]) else 'null'
                # if bid and ask is available show mid price, else use last price
                # but first verify that bid ask spread are not wider than 10%, else use last price
                
                
                if 1 in instr_v.keys() and 2 in instr_v.keys() and 4 in instr_v.keys():
                    
                    bid = float(instr_v[1])
                    ask = float(instr_v[2])
                    mid = (bid + ask) / 2.0
                    tolerance = float(self.rskeys['option.bid_ask_spread_tolerance'])
                    logging.debug( "createImplVolChartData %s %s %s %s %s %s bid/ask ratio %f tolerance level %f" %\
                                         (strike, expiry, right, instr_v[1], instr_v[2], instr_v[4], mid / max(bid, ask),\
                                          tolerance)) 
                    if  (mid / max(bid, ask)) >  tolerance and bid > 0 and ask > 0:
                        matrix[strike][expiry][right][1] = str(mid)
                        
                        
                    else:
                        matrix[strike][expiry][right][1] =  (instr_v[4]) if self.isfloat(instr_v[4]) else 'null'
                        logging.debug( 'createImplVolChartData: unusable bid/ask prices. using last price instead %s' % (instr_v[4]))
                        
#20150915     
# add bid / ask to the the matrix
# [0] delta, [1] mid or last, [2]                                    
                    matrix[strike][expiry][right][2] =  (instr_v[1] if instr_v[1] > 0 else matrix[strike][expiry][right][1]) if self.isfloat(instr_v[1]) else 'null'
                    matrix[strike][expiry][right][3] =  (instr_v[2] if instr_v[2] > 0 else matrix[strike][expiry][right][1]) if self.isfloat(instr_v[2]) else 'null'
                    logging.debug( 'createImplVolChartData: bid/ask prices. bid and ask %s,%s' % \
                                   (str(matrix[strike][expiry][right][2]), str(matrix[strike][expiry][right][3])))
                        
                else:
                    if 4 in instr_v.keys():
                        #print "%s %s %s " % (instr_v[1], instr_v[2], instr_v[4]) 
                        matrix[strike][expiry][right][1] =  (instr_v[4]) if self.isfloat(instr_v[4]) else 'null'
                        matrix[strike][expiry][right][2] =  (instr_v[4]) if self.isfloat(instr_v[4]) else 'null'
                        matrix[strike][expiry][right][3] =  (instr_v[4]) if self.isfloat(instr_v[4]) else 'null'
        
        DataMap.rs.set(DataMap.rskeys['redis.datastore.key.option_implv'], json.dumps(matrix))
        logging.debug("createImplVolChartData: %s" % str( matrix))
        
        # save impl vol to historical time series
        tk = datetime.datetime.now().strftime('%Y%m%d%H%M')        
        DataMap.rs.sadd(self.rskeys['redis.datastore.key.option_implv_ts_set'], tk)
        DataMap.rs.set(tk, json.dumps(matrix))
        logging.debug("createImplVolChartData: saving option implvol matrix key=[%s] " % tk)
        
#       Sample values generated after executing the below:
#
#         ["strike",'P-20150828', 'C-20150828', 'P-20150929', 'C-20150929', ],[22400,0.285061765463,null,0.237686059088,null,],
#         [22600,0.27409953156,null,0.236034059533,0.232365044818,],
#         [22800,0.263230478241,0.320792426398,0.236005003448,0.234991796766,],
#         [23000,0.251675376297,null,0.228237217842,0.230063445797,],
#         [23200,0.241904579328,0.311715835907,0.2242344851,0.226293660467,],
#         [23400,0.227987380777,0.284433386637,0.218600527288,0.203082047571,],
        
#         num_months = len(matrix[strike])
#         s = '["strike",'
#         for i in range(num_months):          
#             s = s + "'P-%s', 'C-%s', " % (matrix[strike].keys()[i], matrix[strike].keys()[i])
#         s = s + '],'
#         for strike, items in sorted(matrix.iteritems()):
#             s = s + '[%s,' % str(strike)
#             l = ''
#             for month, cp in sorted(items.iteritems()):
#                 l = l + ''.join('%s,%s,' % (cp['P'], cp['C']))
#                 #(('0.4f') % (cp['P']) if self.isfloat(cp['P']) else cp['P'], 
#                 #                            ('0.4f') % (cp['C']) if self.isfloat(cp['C']) else cp['C'])
#                                 
#             s = s + l + '],\n'
#         print s               

    def isfloat(self, value):
        try:
          float(value)
          return True
        except ValueError:
          return False

            
    def printContractByID(self, tick_id):
        tick_id = int(tick_id)

        if DataMap().isKeyInMap(tick_id):
    
            
            s = 'Tick:%s [%s]>>' % (tick_id, ContractHelper.printContract(DataMap().get(tick_id)['contract'])) 
            for field, value in DataMap().get(tick_id).iteritems():
                if type(value) is not Contract:
                    s = s + "%d:[%s], " % (field, value)
            print "%s" % s

        print 'values stored in redis:'
        print "%s" % DataMap.rs.get(ContractHelper.makeRedisKey(DataMap().get(tick_id)['contract']))


    def refreshRedisImplVol(self, sec):
        while 1:
            now = int(time.strftime('%H%M'))
            
            #print "%d" % now  + ','.join("%s,%s"% (v[0],v[1]) for k,v in DataMap.mkt_sessions.iteritems())
            
            if (now >= int(DataMap.mkt_sessions['morning'][0]) and now <= int(DataMap.mkt_sessions['morning'][1]) \
                or  now >= int(DataMap.mkt_sessions['afternoon'][0]) and now <= int(DataMap.mkt_sessions['afternoon'][1])):
                DataMap().createImplVolChartData()
                logging.info('refreshRedisImplVol: update redis map every %ds' % sec)
            
            sleep(sec)

    def refresh_portfolio(self, sec):
        p = portfolio.PortfolioManager(self.config)
        
        p.retrieve_position()
#20150914
        #p.recal_port()

        
#        undly_months_prices = eval(self.config.get("market", "option.underlying.month_price").strip('"').strip("'"))
        
        cnt = 0
        while 1:
            now = int(time.strftime('%H%M'))
            if (now >= int(DataMap.mkt_sessions['morning'][0]) and now <= int(DataMap.mkt_sessions['morning'][1]) \
                or  now >= int(DataMap.mkt_sessions['afternoon'][0]) and now <= int(DataMap.mkt_sessions['afternoon'][1])):             
                
                
#20151002
#                     logging.debug('refresh_portfolio: force retrieve all positions from IB!!')

# force retrive again on every run
#                     p.retrieve_position()
                    
                    
# subscribe new contracts traded
#                     toks = map(lambda x: x.split('-'), p.get_pos_contracts())
#             
#                     for t in toks:
#                         contractTuple = (t[0], 'FUT', 'HKFE', 'HKD', '', 0, '')
#                         
#                         #add_subscription(self, contractTuple, month, X, right):
#                         rc = map(lambda x: (omd.add_subscription(x[0], x[1], x[2], x[3])),\
#                                              [(contractTuple, m[0], t[2], 'P') for m in undly_months_prices] +\
#                                              [(contractTuple, m[0], t[2], 'C') for m in undly_months_prices])        
#                 
                     s = p.recal_port()
                     #print s
                    
            sleep(sec)




# class ContractHelper():
#       
#     def __init__(self, contractTuple):
#         self.makeContract(contractTuple)
#       
#       
#     @staticmethod
#     def makeContract(contractTuple):
#         newContract = Contract()
#         newContract.m_symbol = contractTuple[0]
#         newContract.m_secType = contractTuple[1]
#         newContract.m_exchange = contractTuple[2]
#         newContract.m_currency = contractTuple[3]
#         newContract.m_expiry = contractTuple[4]
#         newContract.m_strike = contractTuple[5]
#         newContract.m_right = contractTuple[6]
#         logging.debug( 'Contract Values:%s,%s,%s,%s,%s,%s,%s:' % contractTuple)
#         return newContract
#       
#     @staticmethod
#     def convert2Tuple(newContract):
#         newContractTuple = (newContract.m_symbol,\
#                             newContract.m_secType,\
#                             newContract.m_exchange,\
#                             newContract.m_currency,\
#                             newContract.m_expiry,\
#                             newContract.m_strike,\
#                             newContract.m_right, newContract.m_conId)
#         logging.debug( 'Contract Values:%s,%s,%s,%s,%s,%s,%s %s:' % newContractTuple)
#         return newContractTuple
#       
#   
#     @staticmethod
#     def contract2mapstring(contract):
#         return json.dumps(contract.__dict__)
#   
#   
#   
#     @staticmethod
#     def mapstring2contract(sm_contract):
#           
#         newContract = Contract()
#         mapContract = json.loads(sm_contract)
#         map(lambda x: newContract.__setattr__(x, mapContract[x].encode('ascii') if type(mapContract[x]) == unicode else mapContract[x]), mapContract.keys())
#         return newContract
#   
#   
#     @staticmethod
#     def map2contract(m_contract):
#           
#         newContract = Contract()
#         map(lambda x: newContract.__setattr__(x, m_contract[x].encode('ascii') if type(m_contract[x]) == unicode else m_contract[x]), m_contract.keys())
#         return newContract
#       
#   
#   
#       
#     @staticmethod
#     def printContract(contract):
#         s = '[%s-%s-%s-%s-%s-%s-%s-%s]' % (contract.m_symbol,
#                                                        contract.m_secType,
#                                                        contract.m_exchange,
#                                                        contract.m_currency,
#                                                        contract.m_expiry,
#                                                        contract.m_strike,
#                                                        contract.m_right,
#                                                        contract.m_conId)
#         #logging.info(s)
#         return s
#       
#     @staticmethod
#     def makeRedisKey(contract):
#         #print "makerediskey %s" % ContractHelper.printContract(contract)
# #20150904        
#         contract.m_strike = int(contract.m_strike)
#           
#         if contract.m_secType == 'OPT':
#             s = '%s-%s-%s-%s' % (contract.m_symbol,
#                                                            contract.m_expiry,
#                                                            contract.m_strike,
#                                                            contract.m_right)
#         else:
#             s = '%s-%s-%s-%s' % (contract.m_symbol,
#                                                            contract.m_expiry,
#                                                            contract.m_secType, '')
#               
#         return s
#         
#     @staticmethod
#     def makeRedisKeyEx(contract, old=False):
#         # this routine is to circumvent a problem in makeRedisKey with 
#         # the key in position 3 having different meanings under different conditions.
#         #  
#         # 
#         # to differentiate the keys generated by the old and new functions, 
#         # contract keys created using this routine have their last slot
#         # hard coded a magic number 102
#           
#         if (old):
#             return ContractHelper.makeRedisKey(contract)
#           
#         contract.m_strike = int(contract.m_strike)
# # amend 10/22 
# # add exchange to the key         
#         s = '%s-%s-%s-%s-%s-%s-%s-%d' % (contract.m_symbol,
#                                                            contract.m_expiry,
#                                                            contract.m_strike,
#                                                            contract.m_right,
#                                                            contract.m_secType,
#                                                            contract.m_currency,
#                                                              
#                                                            contract.m_exchange,
#                                                              
#                                                            102)
#         return s



# copy list of subscribed contracts from the console in options_data
# save them into a text file
# call this function to generate subscription txt that 
# can be processed by ib_mds.py
# def create_subscription_file(src, dest):
#     
#     # improper file content will cause
#     # this function to fail
#     f = open(src)
#     lns = f.readlines()
# 
#     a= filter(lambda x: x[0] <> '\n', map(lambda x: x.split(','), lns))
#     contracts = map(lambda x: x.split('-'), [c[1] for c in a])
#     options = filter(lambda x: x[2] <> 'FUT', contracts)
#     futures = filter(lambda x: x[2] == 'FUT', contracts)
#     print contracts
#     #HSI,FUT,HKFE,HKD,20151029,0,
#     futm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'FUT', 'HKFE', 'HKD', x[1], '0', ''), futures)
#     outm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'OPT', 'HKFE', 'HKD', x[1], x[2], x[3]), options)
#     f1 = open(dest, 'w')
#     f1.write(''.join('%s\n'% c for c in outm))
#     f1.write(''.join('%s\n'% c for c in futm))
#     f1.close()

def create_subscription_file(dest):
    
    # improper file content will cause
    # this function to fail
    
    lns = DataMap().printMapCSV().split('\n')

    a= filter(lambda x: x[0] <> '', map(lambda x: x.split(','), lns))
    contracts = map(lambda x: x.split('-'), [c[1] for c in a])
    options = filter(lambda x: x[2] <> 'FUT', contracts)
    futures = filter(lambda x: x[2] == 'FUT', contracts)
    print contracts
    #HSI,FUT,HKFE,HKD,20151029,0,
    futm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'FUT', 'HKFE', 'HKD', x[1], '0', ''), futures)
    outm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'OPT', 'HKFE', 'HKD', x[1], x[2], x[3]), options)
    f1 = open(dest, 'w')
    f1.write(''.join('%s\n'% c for c in outm))
    f1.write(''.join('%s\n'% c for c in futm))
    f1.close()



def console(config, omd):
#    undly_months_prices = eval(config.get("market", "option.underlying.month_price").strip('"').strip("'"))
# 20160612
    undly_months_prices = DataMap().rskeys['option.underlying.month_price']
    
    done = False
    while not done:
#        try:
        print "Available commands are: l - list all subscribed contracts, i - force recal impl vol, s <H/M> <X> - H for HSI M for MHI manual subscription of HKFE options"
        print "                        p - print portfolio summary,   r - rescan portfolio and update subscription list"
        print "                        c [dest] - dump subscribed contracts to an external file"
        print "                       <id> - list subscribed contract by id, q - terminate program"
        cmd = raw_input(">>")
        input = cmd.split(' ')
        if input[0]  == "q":
            done = True
        elif input[0] == 'l' or input[0] == '':
            #DataMap().printMap()
            DataMap().printMapCSV()
            DataMap().createOptionsPanelData()
        elif input[0] == 'i':           
            DataMap().createImplVolChartData()
        elif input[0] == 's':
            symbol = 'HSI' if input[1].upper() == 'H' else 'MHI'
            print symbol
            X = int(input[2])
            contractTuple = (symbol, 'FUT', 'HKFE', 'HKD', '', 0, '')
            #add_subscription(self, contractTuple, month, X, right):
            rc = map(lambda x: (omd.add_subscription(x[0], x[1], x[2], x[3])),\
                                 [(contractTuple, m[0], X, 'P') for m in undly_months_prices] +\
                                 [(contractTuple, m[0], X, 'C') for m in undly_months_prices])
            print 'subscribed items: %s' % rc
        elif input[0] == 'p':
#             port_key  = '%s_%s' % (config.get("redis", "redis.datastore.key.port_prefix").strip('"').strip("'"), \
#                             config.get("redis", "redis.datastore.key.port_summary").strip('"').strip("'"))
            port_key  = config.get("redis", "redis.datastore.key.port_summary").strip('"').strip("'")

            print 'position summary'

            print '\n'.join('%s:\t\t%s' % (k,v) for k,v in sorted(json.loads(DataMap.rs.get(port_key)).iteritems()))
            print '-----end position summary\nl'
        elif input[0] == 'r':
            add_portfolio_subscription(config, omd)
        elif input[0] == 'c':
            create_subscription_file(input[1])
            
        elif isinstance(input[0], int): 
            DataMap().printContractByID(input[0])
        else:
            pass
            
#         except:
#             exc_type, exc_value, exc_traceback = sys.exc_info()
#             traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)


def add_portfolio_subscription(config, omd):
    logging.info('add_portfolio_subscription: subscribe market data for portfolio items')
    p = portfolio.PortfolioManager(config)
    p.retrieve_position()
    
    logging.debug("************ complete retrieve pos ****") 
#    undly_months_prices = eval(config.get("market", "option.underlying.month_price").strip('"').strip("'"))
    # 20160612
    undly_months_prices = DataMap().rskeys['option.underlying.month_price']
    
    # example: MHI-20150929-22600-P
    toks = map(lambda x: x.split('-'), p.get_pos_contracts())
    
    #[['MHI','20150929', 22600, 'P'], [...]]
    for t in toks:
        contractTuple = (t[0], 'FUT', 'HKFE', 'HKD', '', 0, '')
        
        #add_subscription(self, contractTuple, month, X, right):
        rc = map(lambda x: (omd.add_subscription(x[0], x[1], x[2], x[3])),\
                             [(contractTuple, m[0], t[2], 'P') for m in undly_months_prices] +\
                             [(contractTuple, m[0], t[2], 'C') for m in undly_months_prices])        
#         rc = map(lambda x: (omd.add_subscription(x[0], x[1], x[2], x[3])),\
#                         [((tok[0], 'OPT', 'HKFE', 'HKD', '', 0, ''), m[0], tok[2], tok[3]) for tok in toks])
                             
    logging.info('add_portfolio_subscription: add results: %s' % rc)



if __name__ == '__main__':
        
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]    
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0: 
        raise ValueError, "Failed to open config file" 
      
    logconfig = eval(config.get("options_data", "options_data.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    
    DataMap().init_redis(config)
    mdm = MarketDataManager(config)  
    mdm.connect()
    omd = OptionsMarketDataManager(config, mdm)
#     contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '', 0, '')
#     o.option_range(contractTuple, [['20150828', 22817.0], ['20150929', 22715.0]], 200, 0.08)

    add_portfolio_subscription(config, omd)

    thread.start_new_thread(DataMap().refreshRedisImplVol, (60,))
#    thread.start_new_thread(DataMap().refresh_portfolio, (5,))

    thread.start_new_thread(DataMap().refresh_portfolio, (5, ))
    console(config, omd)
  

       
    mdm.disconnect()
    
         
    
