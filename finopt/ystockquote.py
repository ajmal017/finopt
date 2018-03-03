# -*- coding: utf-8 -*-
#
#  Copyright (c) 2007-2008, Corey Goldberg (corey@goldb.org)
#
#  license: GNU LGPL
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.


import urllib, urllib2
import json


"""
This is the "ystockquote" module.

This module provides a Python API for retrieving stock data from Yahoo Finance.

sample usage:
>>> import ystockquote
>>> print ystockquote.get_price('GOOG')
529.46




http://www.gummy-stuff.org/Yahoo-data.htm
a     Ask     a2     Average Daily Volume     a5     Ask Size
b     Bid     b2     Ask (Real-time)     b3     Bid (Real-time)
b4     Book Value     b6     Bid Size     c     Change & Percent Change
c1     Change     c3     Commission     c6     Change (Real-time)
c8     After Hours Change (Real-time)     d     Dividend/Share     d1     Last Trade Date
d2     Trade Date     e     Earnings/Share     e1     Error Indication (returned for symbol changed / invalid)
e7     EPS Estimate Current Year     e8     EPS Estimate Next Year     e9     EPS Estimate Next Quarter
f6     Float Shares     g     Day's Low     h     Day's High
j     52-week Low     k     52-week High     g1     Holdings Gain Percent
g3     Annualized Gain     g4     Holdings Gain     g5     Holdings Gain Percent (Real-time)
g6     Holdings Gain (Real-time)     i     More Info     i5     Order Book (Real-time)
j1     Market Capitalization     j3     Market Cap (Real-time)     j4     EBITDA
j5     Change From 52-week Low     j6     Percent Change From 52-week Low     k1     Last Trade (Real-time) With Time
k2     Change Percent (Real-time)     k3     Last Trade Size     k4     Change From 52-week High
k5     Percebt Change From 52-week High     l     Last Trade (With Time)     l1     Last Trade (Price Only)
l2     High Limit     l3     Low Limit     m     Day's Range
m2     Day's Range (Real-time)     m3     50-day Moving Average     m4     200-day Moving Average
m5     Change From 200-day Moving Average     m6     Percent Change From 200-day Moving Average     m7     Change From 50-day Moving Average
m8     Percent Change From 50-day Moving Average     n     Name     n4     Notes
o     Open     p     Previous Close     p1     Price Paid
p2     Change in Percent     p5     Price/Sales     p6     Price/Book
q     Ex-Dividend Date     r     P/E Ratio     r1     Dividend Pay Date
r2     P/E Ratio (Real-time)     r5     PEG Ratio     r6     Price/EPS Estimate Current Year
r7     Price/EPS Estimate Next Year     s     Symbol     s1     Shares Owned
s7     Short Ratio     t1     Last Trade Time     t6     Trade Links
t7     Ticker Trend     t8     1 yr Target Price     v     Volume
v1     Holdings Value     v7     Holdings Value (Real-time)     w     52-week Range
w1     Day's Value Change     w4     Day's Value Change (Real-time)     x     Stock Exchange
y     Dividend Yield         

"""


def __request(symbol, stat):
    url = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (symbol, stat)
    return urllib.urlopen(url).read().strip().strip('"')


def get_all(symbol):
    """
    Get all available quote data for the given ticker symbol.
    
    Returns a dictionary.
    """
    values = __request(symbol, 'l1c1va2xj1b4j4dyekjm3m4rr5p5p6s7').split(',')
    data = {}
    data['price'] = values[0]
    data['change'] = values[1]
    data['volume'] = values[2]
    data['avg_daily_volume'] = values[3]
    data['stock_exchange'] = values[4]
    data['market_cap'] = values[5]
    data['book_value'] = values[6]
    data['ebitda'] = values[7]
    data['dividend_per_share'] = values[8]
    data['dividend_yield'] = values[9]
    data['earnings_per_share'] = values[10]
    data['52_week_high'] = values[11]
    data['52_week_low'] = values[12]
    data['50day_moving_avg'] = values[13]
    data['200day_moving_avg'] = values[14]
    data['price_earnings_ratio'] = values[15]
    data['price_earnings_growth_ratio'] = values[16]
    data['price_sales_ratio'] = values[17]
    data['price_book_ratio'] = values[18]
    data['short_ratio'] = values[19]
    return data
    
    
def get_price(symbol): 
    return __request(symbol, 'l1')

def get_close(symbol):
    return __request(symbol, 'p')

def get_change(symbol):
    return __request(symbol, 'c1')
    
    
def get_volume(symbol): 
    return __request(symbol, 'v')


def get_avg_daily_volume(symbol): 
    return __request(symbol, 'a2')
    
    
def get_stock_exchange(symbol): 
    return __request(symbol, 'x')
    
    
def get_market_cap(symbol):
    return __request(symbol, 'j1')
   
   
def get_book_value(symbol):
    return __request(symbol, 'b4')


def get_ebitda(symbol): 
    return __request(symbol, 'j4')
    
    
def get_dividend_per_share(symbol):
    return __request(symbol, 'd')


def get_dividend_yield(symbol): 
    return __request(symbol, 'y')
    
    
def get_earnings_per_share(symbol): 
    return __request(symbol, 'e')


def get_52_week_high(symbol): 
    return __request(symbol, 'k')
    
    
def get_52_week_low(symbol): 
    return __request(symbol, 'j')


def get_50day_moving_avg(symbol): 
    return __request(symbol, 'm3')
    
    
def get_200day_moving_avg(symbol): 
    return __request(symbol, 'm4')
    
    
def get_price_earnings_ratio(symbol): 
    return __request(symbol, 'r')


def get_price_earnings_growth_ratio(symbol): 
    return __request(symbol, 'r5')


def get_price_sales_ratio(symbol): 
    return __request(symbol, 'p5')
    
    
def get_price_book_ratio(symbol): 
    return __request(symbol, 'p6')
       
       
def get_short_ratio(symbol): 
    return __request(symbol, 's7')
    
    
def get_historical_prices(symbol, start_date, end_date):
    """
    Get historical prices for the given ticker symbol.
    Date format is 'YYYYMMDD'
    
    Returns a nested list.
    """
    url = 'http://ichart.yahoo.com/table.csv?s=%s&' % symbol + \
          'd=%s&' % str(int(end_date[4:6]) - 1) + \
          'e=%s&' % str(int(end_date[6:8])) + \
          'f=%s&' % str(int(end_date[0:4])) + \
          'g=d&' + \
          'a=%s&' % str(int(start_date[4:6]) - 1) + \
          'b=%s&' % str(int(start_date[6:8])) + \
          'c=%s&' % str(int(start_date[0:4])) + \
          'ignore=.csv'
    days = urllib.urlopen(url).readlines()
    data = [day[:-2].split(',') for day in days]
    return data


def get_alpha_close(symbol):
    av = Alphavantage()
    return av.get_close(symbol)

class Alphavantage():
    
    api_key = 'RH87SY4LF3D62WNA'
    
    
    
    def __init__(self, api_key= None):
        self.api_key = api_key if api_key <> None else self.api_key
    
    def url_request(self, params):
        url = 'https://www.alphavantage.co/query?%s&apikey=%s' % (''.join('&%s=%s' % (k, v) for k, v in params.iteritems()), self.api_key)
        return url
    # TIME_SERIES_DAILY_ADJUSTED
    def time_series_daily_adjusted(self, symbol, datatype=None, outputsize=None):
        url = self.url_request({'function': 'time_series_daily_adjusted', 'symbol':symbol, 'datatype': datatype, 'outputsize':outputsize})
        print url
        if datatype == 'csv':
            days = urllib2.urlopen(url).readlines()
            data = [day[:-2].split(',') for day in days]
            return data
        else:
            return json.loads(urllib2.urlopen(url))
        
        
    def get_close(self, symbol):   
        d = self.time_series_daily_adjusted(symbol, 'csv', 'compact' )
        return d[1][4]
        
   
if __name__ == '__main__':
    av = Alphavantage()
    #d = av.time_series_daily_adjusted('AUDUSD=X', 'csv', 'full')
    d = av.time_series_daily_adjusted('^HSI', 'csv', 'compact')
    #d = [['timestamp', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient'], ['2018-01-09', '3406.1116', '3417.2278', '3403.5869', '3413.8997', '3413.8997', '1968985916', '0.0000', '1.0000'], ['2018-01-08', '3391.5530', '3412.7310', '3384.5591', '3409.4800', '3409.4800', '236200', '0.0000', '1.0000']]
    #print ', '.join('[new Date("%s"), %0.4f]' % (elem[0], (float(elem[4]) - float(elem[1])) / float(elem[4]))  for elem in d[1:])
    print ', '.join('[new Date("%s"), %s]' % (elem[0], float(elem[4]))  for elem in d[1:])
    print len(d)
    
    print get_alpha_close('^HSI')
    
#ss = [["0001.HK",2.5],["0002.HK",1.86],["0003.HK",1.62],["0004.HK",1.27],["0005.HK",15.02],["0006.HK",1.44],["0011.HK",1.42],["0012.HK",0.82],["0013.HK",2.53],["0016.HK",2.53],["0017.HK",0.7],["0019.HK",1.01],["0023.HK",0.67],["0066.HK",0.67],["0083.HK",0.66],["0101.HK",0.94],["0144.HK",0.46],["0151.HK",1.14],["0267.HK",0.18],["0291.HK",0.46],["0293.HK",0.24],["0322.HK",0.71],["0330.HK",0.25],["0386.HK",1.88],["0388.HK",1.86],["0494.HK",1.06],["0688.HK",1.25],["0700.HK",4.52],["0762.HK",0.94],["0836.HK",0.49],["0857.HK",3.29],["0883.HK",4.38],["0939.HK",7.08],["0941.HK",8.08],["1044.HK",0.91],["1088.HK",1.59],["1109.HK",0.53],["1199.HK",0.27],["1299.HK",4.69],["1398.HK",5.25],["1880.HK",1.01],["1898.HK",0.43],["1928.HK",1.06],["2318.HK",1.85],["2388.HK",1.42],["2600.HK",0.19],["2628.HK",2.56],["3328.HK",0.71],["3988.HK",3.63]]
#for s in ss:
#    chg_percent = float(get_change(s[0]))/ float(get_price(s[0])) 
#    print '["%s","HSI",%f,%f],' % (s[0], s[1], chg_percent)


    
        
#ss = [["0001.HK",2.5],["0002.HK",1.86],["0003.HK",1.62],["0004.HK",1.27],["0005.HK",15.02],["0006.HK",1.44],["0011.HK",1.42],["0012.HK",0.82],["0013.HK",2.53],["0016.HK",2.53],["0017.HK",0.7],["0019.HK",1.01],["0023.HK",0.67],["0066.HK",0.67],["0083.HK",0.66],["0101.HK",0.94],["0144.HK",0.46],["0151.HK",1.14],["0267.HK",0.18],["0291.HK",0.46],["0293.HK",0.24],["0322.HK",0.71],["0330.HK",0.25],["0386.HK",1.88],["0388.HK",1.86],["0494.HK",1.06],["0688.HK",1.25],["0700.HK",4.52],["0762.HK",0.94],["0836.HK",0.49],["0857.HK",3.29],["0883.HK",4.38],["0939.HK",7.08],["0941.HK",8.08],["1044.HK",0.91],["1088.HK",1.59],["1109.HK",0.53],["1199.HK",0.27],["1299.HK",4.69],["1398.HK",5.25],["1880.HK",1.01],["1898.HK",0.43],["1928.HK",1.06],["2318.HK",1.85],["2388.HK",1.42],["2600.HK",0.19],["2628.HK",2.56],["3328.HK",0.71],["3988.HK",3.63]]
#for s in ss:
#    chg_percent = float(get_change(s[0]))/ float(get_price(s[0])) 
#    print '["%s","HSI",%f,%f],' % (s[0], s[1], chg_percent)
    
    
        