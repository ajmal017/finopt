#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys, traceback
import json
from time import sleep
from misc2.helpers import ContractHelper
from misc2.observer import Subscriber
from copy import deepcopy
import threading
from urllib3.packages.six import _MovedItems


class AccountSummaryTags():
    '''
        https://interactivebrokers.github.io/tws-api/classIBApi_1_1AccountSummaryTags.html
    '''
    AccountType = "AccountType"
    NetLiquidation = "NetLiquidation"
    TotalCashValue = "TotalCashValue"
    SettledCash = "SettledCash"
    AccruedCash = "AccruedCash"
    BuyingPower = "BuyingPower"
    EquityWithLoanValue = "EquityWithLoanValue"
    PreviousEquityWithLoanValue = "PreviousEquityWithLoanValue"
    GrossPositionValue = "GrossPositionValue"
    ReqTEquity = "ReqTEquity"
    ReqTMargin = "ReqTMargin"
    SMA = "SMA"
    InitMarginReq = "InitMarginReq"
    MaintMarginReq = "MaintMarginReq"
    AvailableFunds = "AvailableFunds"
    ExcessLiquidity = "ExcessLiquidity"
    Cushion = "Cushion"
    FullInitMarginReq = "FullInitMarginReq"
    FullMaintMarginReq = "FullMaintMarginReq"
    FullAvailableFunds = "FullAvailableFunds"
    FullExcessLiquidity = "FullExcessLiquidity"
    LookAheadNextChange = "LookAheadNextChange"
    LookAheadInitMarginReq = "LookAheadInitMarginReq"
    LookAheadMaintMarginReq = "LookAheadMaintMarginReq"
    LookAheadAvailableFunds = "LookAheadAvailableFunds"
    LookAheadExcessLiquidity = "LookAheadExcessLiquidity"
    HighestSeverity = "HighestSeverity"
    DayTradesRemaining = "DayTradesRemaining"
    Leverage = "Leverage"
    
    
    @staticmethod
    def get_all_tags():
        return ','.join('%s' % tag for tag in filter(lambda t: '__' not in t, dir(AccountSummaryTags)))

    

class AccountPositionTrackerException(Exception):
    pass

class AccountPositionTracker(Subscriber):
    
    POSITION_EVENTS = ['position', 'accountSummary', 'update_portfolio_account']
    
    def __init__(self, name, gw_parent):

        Subscriber.__init__(self, name)
        self.name = name
        # indicate end of position download
        self.pos_end = False 
        # indicate end of acct summary download
        self.acct_end = False
        # indicate end of port acct download
        self.port_acct_end = {}
        self.account_position = {}
        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        
        '''
             ask tws_event_handler to forward tick messages to
             this class
             
        '''
        for e in AccountPositionTracker.POSITION_EVENTS:
            self.tws_event_handler.register(e, self)                  
    
    def handle_portfolio_account(self, **items):
        '''
            
            in tws_event_handler, the update_portfolio_account message is used for handling the four types 
            of tws events below, all these events are sent as "update_portfolio_account" event 
            to find out which event is being sent by update_portfolio_account, look into 
            the param for the key "tws_event"
             
            refer to line 142 in the source file:
                self.update_portfolio_account(
                              {'tws_event': 'updatePortfolio'....
                              
            Note that each tws event carried different params, as such for different event
            the **items will contain different things
        
            def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, account):
            def updateAccountValue(self, key, value, currency, account):
            def updateAccountTime(self, timeStamp):
            def accountDownloadEnd(self, account):
            
            the variables are stored as below:
                retrieve the accountName in the event
                
                self.account_position[accountName]['lastUpdated'] = timeStamp
                
            
        '''
        ev = items['tws_event']
            
          
        if ev == 'accountDownloadEnd':
            self.port_acct_end[items['account']] = True
        elif ev == 'updateAccountTime':
            try:
                for a in self.account_position.keys():
                    self.account_position[a]['portfolio_account']['timestamp'] = items['timestamp']
                #map(lambda x: x['portfolio_account'].__setitem__('timestamp', items['timeStamp']), self.account_position)
            except:
                #pass
                logging.error(traceback.format_exc())

        else:
            try:
                account = items['account']
                _ = self.account_position[account]
            except:
                self.account_position[account] = {'acct_position': {}, 
                                                               'account_summary': {'tags':{}},
                                                               'portfolio_account': {'tags':{}, 'contracts':[]}
                                                               }
                
            if ev == 'updateAccountValue':
                self.account_position[account]['portfolio_account']['tags'][items['key']] = items['value'] 
                self.account_position[account]['portfolio_account']['tags']['currency'] = items['currency']
            elif ev == 'updatePortfolio':
                try:
                    contract_key = items['contract_key']
                    self.account_position[account]['portfolio_account']
                    ckv = ContractHelper.contract2kv(ContractHelper.makeContractfromRedisKeyEx(contract_key)) 
                    self.account_position[account]['portfolio_account'][contract_key] = {'contract': ckv,
                                                                  'position': items['position'], 
                                                                  'market_price': items['market_price'],
                                                                  'market_value': items['market_value'],
                                                                  'average_cost': items['average_cost'],
                                                                  'unrealized_PNL': items['unrealized_PNL'],
                                                                  'realized_PNL': items['realized_PNL'],
                                                                  
                                                                  }
                except:
                    logging.error(traceback.format_exc())
       
    def handle_position(self, account, contract_key, position, average_cost, end_batch):
        logging.info('AccountPositionTracker:position account[%s] contract[%s]', account, contract_key)
        if not end_batch:
            self.pos_end = False
            ckv = ContractHelper.contract2kv(ContractHelper.makeContractfromRedisKeyEx(contract_key)) 
            try:
                _ = self.account_position[account]                
            except:
                #logging.error(traceback.format_exc())
                self.account_position[account] = {'acct_position': {contract_key: {'contract': ckv, 'position': position, 'average_cost': average_cost}},
                                                  'account_summary': {'tags':{}},
                                                  'portfolio_account':{'tags':{}, 'contracts':[]}
                                                 }
            
            

            self.account_position[account]['acct_position'][contract_key] = {'contract': ckv,
                                                              'position': position, 'average_cost': average_cost}
            
            
                        
        else:
            self.pos_end = True
                    
    def handle_account_summary(self, **items):       
        if not items['end_batch']:     
            self.acct_end = False 
            try:
                _ = self.account_position[items['account']]
            except KeyError:
                self.account_position[items['account']]= {'acct_position': {}, 
                                                          'account_summary': {'tags':{}},
                                                          'portfolio_account': {'tags':{}, 'contracts':[]}
                                                           }
                
            for k, v in items.iteritems():
                if k == 'tag':
                    self.account_position[items['account']]['account_summary']['tags'][v] = items['value'] 
                elif k == 'value':
                    pass
                else:
                    self.account_position[items['account']]['account_summary'][k] = v 
        else:
            self.acct_end = True
                         
    def update(self, event, **param): 
        if event == 'position':
            self.handle_position(**param)
        elif event == 'accountSummary':
            self.handle_account_summary(**param)
        elif event == 'update_portfolio_account':
            self.handle_portfolio_account(**param)
   
   
    def get_positions(self, account=None):
        lock = threading.RLock()
        result = None
        try:
            lock.acquire()
            i = 0
            while self.pos_end == False or self.acct_end == False:
                sleep(0.5)
                i += 1
                logging.warn('AccountPositionTracker: waiting position status to change to finish: round # %d... ' % i)
                if i == 10:
                    raise AccountPositionTrackerException
            try:
                result= self.account_position[account]
            except:
                result =  self.account_position
        except AccountPositionTrackerException:
            logging.error('AccountPositionTracker: get_positions time out exception %s' % traceback.format_exc())
        except:
            logging.error('AccountPositionTracker: %s' % traceback.format_exc())
        finally:
            lock.release()
            return result     
        
    def get_accounts(self):
        return self.account_position.keys()
           