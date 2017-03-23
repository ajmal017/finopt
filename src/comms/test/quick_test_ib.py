import sys
import copy
from time import sleep, strftime
import time, datetime
import ConfigParser
from optparse import OptionParser
import logging
import thread
import threading
import traceback
import json
from threading import Lock
from ib.ext.Contract import Contract
from ib.ext.EWrapper import EWrapper
from ib.ext.EClientSocket import EClientSocket
from misc2.helpers import ContractHelper

class Wrapger(EWrapper):
    def tickPrice(self, tickerId, field, price, canAutoExecute):
        """ generated source for method tickPrice """
        print vars()
   
    def tickSize(self, tickerId, field, size):
        """ generated source for method tickSize """
        print vars()
   
    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        """ generated source for method tickOptionComputation """

   
    def tickGeneric(self, tickerId, tickType, value):
        """ generated source for method tickGeneric """

   
    def tickString(self, tickerId, tickType, value):
        """ generated source for method tickString """

   
    def tickEFP(self, tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureExpiry, dividendImpact, dividendsToExpiry):
        """ generated source for method tickEFP """

   
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld):
        """ generated source for method orderStatus """

   
    def openOrder(self, orderId, contract, order, orderState):
        """ generated source for method openOrder """

   
    def openOrderEnd(self):
        """ generated source for method openOrderEnd """

   
    def updateAccountValue(self, key, value, currency, accountName):
        """ generated source for method updateAccountValue """

   
    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        """ generated source for method updatePortfolio """

   
    def updateAccountTime(self, timeStamp):
        """ generated source for method updateAccountTime """

   
    def accountDownloadEnd(self, accountName):
        """ generated source for method accountDownloadEnd """

   
    def nextValidId(self, orderId):
        """ generated source for method nextValidId """

   
    def contractDetails(self, reqId, contractDetails):
        """ generated source for method contractDetails """

   
    def bondContractDetails(self, reqId, contractDetails):
        """ generated source for method bondContractDetails """

   
    def contractDetailsEnd(self, reqId):
        """ generated source for method contractDetailsEnd """

   
    def execDetails(self, reqId, contract, execution):
        """ generated source for method execDetails """

   
    def execDetailsEnd(self, reqId):
        """ generated source for method execDetailsEnd """

   
    def updateMktDepth(self, tickerId, position, operation, side, price, size):
        """ generated source for method updateMktDepth """

   
    def updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
        """ generated source for method updateMktDepthL2 """

   
    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        """ generated source for method updateNewsBulletin """

   
    def managedAccounts(self, accountsList):
        """ generated source for method managedAccounts """

   
    def receiveFA(self, faDataType, xml):
        """ generated source for method receiveFA """

   
    def historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        """ generated source for method historicalData """

   
    def scannerParameters(self, xml):
        """ generated source for method scannerParameters """

   
    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        """ generated source for method scannerData """

   
    def scannerDataEnd(self, reqId):
        """ generated source for method scannerDataEnd """

   
    def realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
        """ generated source for method realtimeBar """

   
    def currentTime(self, time):
        """ generated source for method currentTime """

   
    def fundamentalData(self, reqId, data):
        """ generated source for method fundamentalData """

   
    def deltaNeutralValidation(self, reqId, underComp):
        """ generated source for method deltaNeutralValidation """

   
    def tickSnapshotEnd(self, reqId):
        """ generated source for method tickSnapshotEnd """

   
    def marketDataType(self, reqId, marketDataType):
        """ generated source for method marketDataType """

   
    def commissionReport(self, commissionReport):
        """ generated source for method commissionReport """

   
    def position(self, account, contract, pos, avgCost):
        """ generated source for method position """

   
    def positionEnd(self):
        """ generated source for method positionEnd """

   
    def accountSummary(self, reqId, account, tag, value, currency):
        """ generated source for method accountSummary """

   
    def accountSummaryEnd(self, reqId):
        """ generated source for method accountSummaryEnd """
    
    def connectionClosed(self):
        """ generated source for method accountSummaryEnd """

    def error(self, id=None, errorCode=None, errorMsg=None):
        """ generated source for method accountSummaryEnd """
        print errorMsg

    def error_0(self, strvalue=None):
        """ generated source for method accountSummaryEnd """

    def error_1(self, id=None, errorCode=None, errorMsg=None):
        """ generated source for method accountSummaryEnd """
    

def test_IB():
    ew = Wrapger()
    es = EClientSocket(ew)
    es.eConnect('localhost', 7496, 5555)
    print es.isConnected()
    
    contractTuple = ('HSI', 'FUT', 'HKFE', 'HKD', '20170330', 0, '')
    contract = ContractHelper.makeContract(contractTuple)
    es.reqMktData(1000, contract, '', True) 
    es.reqMktData(0, contract, '', True)

    sleep(5)
    print 'disconnecting...'
    es.eDisconnect()

class x:
    def f1(self, a, b, c, d):
        print vars()

if __name__ == '__main__':
    


    test_IB()
#     y = x()
#     y.f1(1, 2, {4:5}, [77,88])
#     vv = {'a': 1, 'c': {4: 5}, 'b': 2, 'd': [6,77, 88]}    
#     y.f1(**vv)