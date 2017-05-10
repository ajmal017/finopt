# -*- coding: utf-8 -*-


import json
import logging
import threading
import ConfigParser
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.ExecutionFilter import ExecutionFilter


class BaseHelper():
    @staticmethod
    def object2kvstring(o):
        return json.dumps(o.__dict__)
        
    @staticmethod
    def kvstring2object(kvstring, object):
        return BaseHelper.kv2object(json.loads(kvstring), object)


    @staticmethod
    def kv2object(kv, Object):   
        o = Object()
        map(lambda x: o.__setattr__(x, kv[x].encode('ascii') if type(kv[x]) == unicode else kv[x]), kv.keys())
        return o
        
        
class OrderHelper(BaseHelper):
    pass
     
#     @staticmethod
#     def object2kvstring(contract):
#         return json.dumps(contract.__dict__)
#  
#     @staticmethod
#     def kvstring2object(sm_order):
#         return OrderHelper.kv2object(json.loads(sm_order))
#  
#  
#     @staticmethod
#     def kv2object(m_order):   
#         newOrder = Order()
#         map(lambda x: newOrder.__setattr__(x, m_order[x].encode('ascii') if type(m_order[x]) == unicode else m_order[x]), m_order.keys())
#         return newOrder

    
class ExecutionFilterHelper(BaseHelper):
    @staticmethod
    def makeExeuction(executionTuple):
        '''
        String m_acctNumber    The customer account number.
        double m_avgPrice    Average price. Used in regular trades, combo trades and legs of the combo. Does not include commissions.
        int m_clientId    "The id of the client that placed the order.
        Note: TWS orders have a fixed client id of ""0."""
        int m_cumQty    Cumulative quantity. Used in regular trades, combo trades and legs of the combo.
        String m_exchange    Exchange that executed the order.
        String m_execId    Unique order execution id.
        int m_liquidation    Identifies the position as one to be liquidated last should the need arise.
        int m_orderId    "The order id.
        Note:  TWS orders have a fixed order id of ""0."""
        int m_permId    The TWS id used to identify orders, remains the same over TWS sessions.
        double m_price    The order execution price, not including commissions.
        int m_shares    The number of shares filled.
        String m_side    "Specifies if the transaction was a sale or a purchase. Valid values are:
        BOT
        SLD"
        String m_time    The order execution time.

        '''
        
        pass


    @staticmethod
    def makeExeuctionFilter(executionFilterTuple):
        '''
        String m_acctCode    Filter the results of the reqExecutions() method based on an account code. Note: this is only relevant for Financial Advisor (FA) accounts.
        int m_clientId    Filter the results of the reqExecutions() method based on the clientId.
        String m_exchange    Filter the results of the reqExecutions() method based on theorder exchange.
        String m_secType    "Filter the results of the reqExecutions() method based on the order security type.
        Note: Refer to the Contract struct for the list of valid security types."
        String m_side    "Filter the results of the reqExecutions() method based on the order action.
        Note: Refer to the Order class for the list of valid order actions."
        String m_symbol    Filter the results of the reqExecutions() method based on the order symbol.
        String m_time    "Filter the results of the reqExecutions() method based on execution reports received after the specified time.
        The format for timeFilter is ""yyyymmdd-hh:mm:ss"""
        '''
        new_filter = ExecutionFilter()
        new_filter.m_acctCode = executionFilterTuple[0]
        new_filter.m_clientId = executionFilterTuple[1] 
        new_filter.m_exchange = executionFilterTuple[2]
        new_filter.m_secType = executionFilterTuple[3]
        new_filter.m_side = executionFilterTuple[4]
        new_filter.m_symbol = executionFilterTuple[5]
        new_filter.m_time = executionFilterTuple[6]
        return new_filter


class ContractHelper(BaseHelper):
    
    map_rules = {'exchange': {'HSI': 'HKFE', 'MHI': 'HKFE'}}
    
    def __init__(self, contractTuple):
        self.makeContract(contractTuple)
    
    
    @staticmethod
    def makeContract(contractTuple):
        newContract = Contract()
        newContract.m_symbol = contractTuple[0]
        newContract.m_secType = contractTuple[1]
        newContract.m_exchange = contractTuple[2]
        newContract.m_currency = contractTuple[3]
        newContract.m_expiry = contractTuple[4]
        newContract.m_strike = contractTuple[5]
        newContract.m_right = contractTuple[6]
        logging.debug( 'Contract Values:%s,%s,%s,%s,%s,%s,%s:' % contractTuple)
        return newContract
    
    @staticmethod
    def convert2Tuple(newContract):
        newContractTuple = (newContract.m_symbol,\
                            newContract.m_secType,\
                            newContract.m_exchange,\
                            newContract.m_currency,\
                            newContract.m_expiry,\
                            newContract.m_strike,\
                            newContract.m_right, newContract.m_conId)
        logging.debug( 'Contract Values:%s,%s,%s,%s,%s,%s,%s %s:' % newContractTuple)
        return newContractTuple
    

    @staticmethod
    def contract2kvstring(contract):
        return json.dumps(contract.__dict__)



    @staticmethod
    def kvstring2contract(sm_contract):
        
        return ContractHelper.kv2contract(json.loads(sm_contract))


    @staticmethod
    def kv2contract(m_contract):
        
        newContract = Contract()
        map(lambda x: newContract.__setattr__(x, m_contract[x].encode('ascii') if type(m_contract[x]) == unicode else m_contract[x]), m_contract.keys())
        return newContract
    


    
    @staticmethod
    def printContract(contract):
        s = '[%s-%s-%s-%s-%s-%s-%s-%s]' % (contract.m_symbol,
                                                       contract.m_secType,
                                                       contract.m_exchange,
                                                       contract.m_currency,
                                                       contract.m_expiry,
                                                       contract.m_strike,
                                                       contract.m_right,
                                                       contract.m_conId)
        #logging.info(s)
        return s
    
    @staticmethod
    def makeRedisKey(contract):
        #print "makerediskey %s" % ContractHelper.printContract(contract)
#20150904        
        contract.m_strike = int(contract.m_strike)
        #contract.m_strike = contract.m_strike
        
        if contract.m_secType == 'OPT':
            s = '%s-%s-%s-%s' % (contract.m_symbol,
                                                           contract.m_expiry,
                                                           contract.m_strike,
                                                           contract.m_right)
        else:
            s = '%s-%s-%s-%s' % (contract.m_symbol,
                                                           contract.m_expiry,
                                                           contract.m_secType, '')
            
        return s
      
    @staticmethod
    def makeRedisKeyEx(contract, old=False):
        # this routine is to circumvent a problem in makeRedisKey with 
        # the key in position 3 having different meanings under different conditions.
        #  
        # 
        # to differentiate the keys generated by the old and new functions, 
        # contract keys created using this routine have their last slot
        # hard coded a magic number 102
        
        if (old):
            return ContractHelper.makeRedisKey(contract)
        
        #contract.m_strike = int(contract.m_strike)
        #contract.m_strike = contract.m_strike
# amend 10/22 
# add exchange to the key         
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
        
# amend 12/1
#change strike format to 2 dp     

# amend 2017/04/25
        #if contract.m_exchange == None or contract.m_exchange == '':
        
# amend 2017/05/10
        # only symbols with type = HSI/MHI will have the exchange key assigned 
        # any other symbols will have their exchange set to empty
        # this is to handle US stocks which could be traded on different exchanges
        # and thus result in different exchange key but in reality they also refer
        # to the same stock        
        #
        # example: FXP--0.00-0-STK-USD--102
        #          FXP--0.00--STK-USD-ARCA-102
        #  
        try:
            contract.m_exchange = ContractHelper.map_rules['exchange'][contract.m_symbol]
        except:
            contract.m_exchange = '' 
            
        

# amend 2017/05/10
   
        '''
            the contract object returned by TWS/IB API is not consistent between
            different messages (position message and account messages)
        '''
        if contract.m_right == None or contract.m_right == '0' or contract.m_right == 0:
            contract.m_right = ''
            
        if contract.m_expiry == None:
            contract.m_expiry = ''
            
                        
        s = '%s-%s-%.2f-%s-%s-%s-%s-%d' % (contract.m_symbol,
                                                           contract.m_expiry,
                                                           float(contract.m_strike),
                                                           contract.m_right,
                                                           contract.m_secType,
                                                           contract.m_currency,
                                                           contract.m_exchange,
                                                            
                                                           102)
        
        return s
    
    
    @staticmethod
    def is_equal(c1, c2):
        return ContractHelper.makeRedisKeyEx(c1) == ContractHelper.makeRedisKeyEx(c2) 

    @staticmethod
    def makeContractfromRedisKeyEx(key):
        
        def utf2asc(x):
            return x.encode('ascii') if isinstance(x, unicode) else x
                
                #return map(lambda x: (x[0], ContractHelper.kvstring2contract(utf2asc(x[1]))), id_contracts)
        
        toks = utf2asc(key).split('-')
        c = Contract()
        c.m_symbol = toks[0]
        c.m_expiry = toks[1]
        c.m_strike = float(toks[2])
        c.m_right = toks[3]
        c.m_secType = toks[4]
        c.m_currency = toks[5]
        c.m_exchange = toks[6]               
        return c 
    
# def str2dict(s):
#     return ast.literal_eval(s)

def dict2str(dict):
    # enclose strings in double quotes
    return '{'  + ', '.join('"%s" : %s' % (k, '"%s"' % v if type(v) == str else v) for k, v in dict.iteritems()) + '}'   

class HelperFunctions():
    @staticmethod
    def utf2asc(x):
        return x.encode('ascii') if isinstance(x, unicode) else x
    

class ConfigMap():
    
    def kwargs_from_file(self, path):
        cfg = ConfigParser.ConfigParser()            
        if len(cfg.read(path)) == 0: 
            raise ValueError, "Failed to open config file [%s]" % path 

        kwargs = {}
        for section in cfg.sections():
            optval_list = map(lambda o: (o, cfg.get(section, o)), cfg.options(section)) 
            for ov in optval_list:
                try:
                    
                    kwargs[ov[0]] = eval(ov[1])
                except:
                    continue
                
        #logging.debug('ConfigMap: %s' % kwargs)
        return kwargs
    

