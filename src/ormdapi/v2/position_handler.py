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
    
    POSITION_EVENTS = ['position', 'accountSummary']
    
    def __init__(self, name, gw_parent):

        Subscriber.__init__(self, name)
        self.name = name
        self.pos_end = False 
        self.acct_end = False
        self.account_position = {}
        self.gw_parent = gw_parent
        self.tws_event_handler = gw_parent.get_tws_event_handler()
        
        '''
             ask tws_event_handler to forward tick messages to
             this class
             
        '''
        for e in ['position', 'accountSummary']:
            self.tws_event_handler.register(e, self)                  
    
        
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
                                                  'account_summary': {'tags':{}}}
            
            

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
                self.account_position[items['account']]= {'acct_position': {}, 'account_summary': {'tags':{}}}
                
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
           