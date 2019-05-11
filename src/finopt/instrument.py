import logging
from ib.ext.Contract import Contract
from misc2.helpers import ContractHelper, dict2str

class InstrumentIdMap():
    
    idmap = {0: 'BSIZE',
                    1: 'BID',
                    2: 'ASK',
                    3: 'ASIZE',
                    4: 'LAST',
                    5: 'LSIZE',
                    6: 'HIGH',
                    7: 'LOW',
                    8: 'VOLUM',
                    9: 'CLOSE',
                    14: 'OPEN',
                    5001: 'IVOL',
                    5002: 'DELTA',
                    5003: 'GAMA',
                    5004: 'THETA',
                    5005: 'VEGA',
                    5006: 'PREMIUM',
                    7001: 'POS',
                    7002: 'AVGCST',
                    7003: 'PDLTA',
                    7004: 'PTHTA',
                    7009: 'GAMA%',
                    7005: 'P/L',
                    7006: '%GAIN',
                    7007: 'AVGPX',
                    7008: 'MKTVAL',
                    7041: 'PTGAIN',
                    9000: 'TDLTA',
                    9001: 'TDL_F',
                    9002: 'TDL_C',
                    9003: 'TDL_P',
                    9010: 'TTHEA',
                    9012: 'TTHEAC',
                    9013: 'TTHEAP',
                    9020: 'TGAM%',
                    9031: 'NUMC',
                    9032: 'NUMP',
                    9040: 'T_UNRPL',
                    9041: 'TPTGAIN',
                    9051: 'AVILFUNDS',
                    9052: 'T_CASH'
                    
                    }
    
    @staticmethod
    def id2str(id):
        try:
            
            return InstrumentIdMap.idmap[id]
        except:
            return str(id)

class Symbol():
    key = None
    
    
    
    '''
        Available tick types
        https://interactivebrokers.github.io/tws-api/tick_types.html
    
    '''
    LAST = 4
    BID  = 1
    ASK  = 2
    BIDSIZE = 0
    ASKSIZE = 3
    LASTSIZE = 5
    CLOSE = 9
    VOLUME = 8
    HIGH = 6
    LOW = 7
    OPEN_TICK =14
    
    
    #https://interactivebrokers.github.io/tws-api/tick_types.html
    BID_OPTION= 10
    ASK_OPTION= 11
    LAST_OPTION=12
    #def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
    
    def __init__(self, contract):
        self.contract = contract
        self.tick_values = {}
        '''
            ib_option_greeks:
            key in 10,11,12 and value is a dict of impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice
        '''
        self.ib_option_greeks = {} 
        self.extra = {}
        self.key = ContractHelper.makeRedisKeyEx(contract)    
    
    def set_tick_value(self, id, price):
        self.tick_values[id] = price

    def get_tick_value(self, id):
        try:
            
            return self.tick_values[id]
    
        except:
            
            return None

    def get_contract(self):
        return self.contract
    
    def get_tick_values(self):
        return self.tick_values
    
    def set_extra_attributes(self, id, val):
        self.extra[id] = val
            
    def get_extra_attributes(self):
        return self.extra
    
    def get_key(self):
        return self.key
    
    def set_ib_option_greeks(self, id, greeks):
        for k, v in greeks.iteritems():
            # overflow 9223372036854775808
            if v > 9223372036854700000:
                greeks[k] = float('nan')
        self.ib_option_greeks[id] = greeks
        
    def get_ib_option_greeks(self, id):
        return self.ib_option_greeks[id]
    
class Option(Symbol):
    """
        Tick Value      Description
        # 5001            impl vol
        # 5002            delta
        # 5003            gamma
        # 5004            theta
        # 5005            vega
        # 5006            premium    
    """
    
    
    IMPL_VOL = 5001
    DELTA    = 5002
    GAMMA    = 5003
    THETA    = 5004
    VEGA     = 5005
    PREMIUM  = 5006
    

    #[0,1,2,3,4,5,6,7,8,9,14,5001,5002,5003,5004,5005,5006]
         

    def __init__(self, contract):
        Symbol.__init__(self, contract)
        
        #self.set_analytics(-1.0, -1.0, -1.0, -1.0, -1.0, -1.0)
        self.set_analytics(float('nan'),float('nan'),float('nan'),float('nan'),float('nan'))

        
    def set_analytics(self, imvol=None, delta=None, gamma=None, theta=None, vega=None, npv=None):
        
        
#         if self.analytics == None:
#             self.analytics = {}           
#         self.analytics[Option.IMPL_VOL] = imvol
#         self.analytics[Option.DELTA] = delta 
#         self.analytics[Option.GAMMA] = gamma
#         self.analytics[Option.THETA] = theta
#         self.analytics[Option.VEGA] = vega
#         self.analytics[Option.PREMIUM] = npv
        self.set_tick_value(Option.IMPL_VOL, imvol)
        self.set_tick_value(Option.DELTA, delta) 
        self.set_tick_value(Option.GAMMA, gamma)
        self.set_tick_value(Option.THETA, theta)
        self.set_tick_value(Option.VEGA, vega)
        self.set_tick_value(Option.PREMIUM, npv)        
        
        
        
    def get_analytics(self):
        
    
        return self.get_tick_value(Option.IMPL_VOL),\
                self.get_tick_value(Option.DELTA), \
                self.get_tick_value(Option.GAMMA),\
                self.get_tick_value(Option.THETA),\
                self.get_tick_value(Option.VEGA),\
                self.get_tick_value(Option.PREMIUM)   
    
    
    def get_strike(self):
        return self.get_contract().m_strike
    
    def object2kvstring(self):
        
        raise Exception
    
    
        try:           
            kv = self.object2kv()
            return '{"%s":%s, "%s":%s, "%s":%s}' % ('contract', ContractHelper.contract2kvstring(self.get_contract()), 
                                                    'tick_values', dict2str(kv['tick_values']),
                                                    'extra', dict2str(kv['extra']))
        except:
            logging.error( 'Exception Option.object2kvstring')
               
        return None
    
    
    def object2kv(self):
        
        
        try:
            
            contract =  self.get_contract()
            tick_values = self.get_tick_values()
            extra = self.get_extra_attributes()
            return {'contract': contract, 'tick_values': tick_values, 'extra': extra}            
        except:
            logging.error( 'Exception Option.object2kv')
               
        return None
      
      
class ExecFill:
    
    SIDE_B = 'BOT'
    SIDE_S = 'SLD'
    
    UNDLY_PX = 3001
    EXEC_IV = 3002
    
    def __init__(self):
        self.extra = {}

              
    def setValues(self, req_id, contract_key, order_id, side, price, avg_price, cum_qty, exec_id, account, exchange, exec_time, order_ref):
        self.req_id = req_id
        self.contract_key = contract_key
        self.order_id = order_id
        self.side = side
        self.price = price
        self.avg_price = avg_price
        self.cum_qty = cum_qty
        self.exec_id = exec_id
        self.account = account
        self.exchange = exchange
        self.exec_time = exec_time
        self.order_ref = order_ref

    def set_extra_attributes(self, id, val):
        self.extra[id] = val
            
    def get_extra_attributes(self):
        return self.extra    
    
    def set_impl_vol(self, iv):
        self.set_extra_attributes(ExecFill.EXEC_IV, iv)
    
    def set_undly_spot(self, px):
        self.set_extra_attributes(ExecFill.UNDLY_PX, px)
        
    def get_kv(self):
        return self.__dict__
    
    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__
    
