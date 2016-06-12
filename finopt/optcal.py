# -*- coding: utf-8 -*-

from QuantLib import *
from bs4 import BeautifulSoup
from urllib2 import urlopen, Request
from time import strftime
import time
import traceback


def cal_implvol(spot, strike, callput, evaldate, exdate, rate, div, vol, premium):
    
    Settings.instance().evaluationDate = str2qdate(evaldate)
    exercise = EuropeanExercise(str2qdate(exdate))
    payoff = PlainVanillaPayoff(str2qopt_type(callput), strike)
    option = EuropeanOption(payoff,exercise)
    S = QuoteHandle(SimpleQuote(spot))
#     r = YieldTermStructureHandle(FlatForward(0, HongKong(), rate, Actual365Fixed()))
#     q = YieldTermStructureHandle(FlatForward(0, HongKong(), div, Actual365Fixed()))
    r = YieldTermStructureHandle(FlatForward(str2qdate(evaldate), rate, Actual365Fixed()))
    q = YieldTermStructureHandle(FlatForward(str2qdate(evaldate), div, Actual365Fixed()))

    sigma = BlackVolTermStructureHandle(BlackConstantVol(str2qdate(evaldate), HongKong(), vol, Actual365Fixed()))
    process = BlackScholesMertonProcess(S,q,r,sigma)
    im = option.impliedVolatility(premium, process)
    results = {}
    results['imvol'] = im
 

    return results


def cal_option(spot, strike, callput, evaldate, exdate, rate, div, vol):
    
    Settings.instance().evaluationDate = str2qdate(evaldate)
    exercise = EuropeanExercise(str2qdate(exdate))
    payoff = PlainVanillaPayoff(str2qopt_type(callput), strike)
    option = EuropeanOption(payoff,exercise)
    S = QuoteHandle(SimpleQuote(spot))
#    r = YieldTermStructureHandle(FlatForward(0, HongKong(), rate, Actual365Fixed()))
#    q = YieldTermStructureHandle(FlatForward(0, HongKong(), div, Actual365Fixed()))
    r = YieldTermStructureHandle(FlatForward(str2qdate(evaldate), rate, Actual365Fixed()))
    q = YieldTermStructureHandle(FlatForward(str2qdate(evaldate), div, Actual365Fixed()))

#    sigma = BlackVolTermStructureHandle(BlackConstantVol(0, HongKong(), vol, Actual365Fixed()))
    sigma = BlackVolTermStructureHandle(BlackConstantVol(str2qdate(evaldate), HongKong(), vol, Actual365Fixed()))
    process = BlackScholesMertonProcess(S,q,r,sigma)
    engine = AnalyticEuropeanEngine(process)
    option.setPricingEngine(engine)
            
    results = {}
    results['npv'] = option.NPV()

    results['delta'] = option.delta()
    results['gamma'] = option.gamma()
    
    results['theta'] = option.theta() / 365
    results['vega'] = option.vega() 
#    results['rho'] = option.rho() 

    results['strikeSensitivity'] = option.strikeSensitivity()
   # results['thetaPerDay'] = option.thetaPerDay()
   # results['itmCashProbability'] = option.itmCashProbability()
 

    return results
    
        


def str2qdate(yyyymmdd):
    months = [January, February, March, April, May, June, July, August, September, October,
                November, December]
    #print '%d%d%d'% (int(yyyymmdd[6:8]), int(yyyymmdd[4:6])-1 , int(yyyymmdd[0:4])) 
    return Date(int(yyyymmdd[6:8]), months[int(yyyymmdd[4:6])-1 ], int(yyyymmdd[0:4]))
    

def qdate2str(dd):
    return '%s%s%s' % (dd.ISO()[0:4], dd.ISO()[5:7], dd.ISO()[8:10])
    

def str2qopt_type(callput):
    if callput.upper() == 'C':
        return Option.Call
    return Option.Put



def get_hk_holidays(year):
    month_names = {'January' : 1,
                    'February': 2,
                    'March': 3,
                    'April': 4,
                    'May': 5,
                    'June': 6,
                    'July': 7,
                    'August': 8,
                    'September': 9,
                    'October': 10,
                    'November': 11,
                    'December': 12,
                    } 
    try:
        url = 'http://www.gov.hk/en/about/abouthk/holiday/{{year}}.htm'
        #url = 'http://www.gov.hk/en/about/abouthk/holiday/'
        url = url.replace('{{year}}', str(year))


        headers = { 'User-Agent' : 'Mozilla/5.0' }
        req = Request(url, None, headers)
        html = urlopen(req).read()

        
                
        
        
        soup = BeautifulSoup(html, 'html5lib')
        
        tds = soup.findAll('h3')[0].parent.findAll('td', 'date')
        
        d1 = map(lambda x: (int(x.text.split(' ')[0]), x.text.split(' ')[1]), tds[1:])
        holidays =  map(lambda x: '%d%02d%02d' % (year, int(month_names[x[1]]), int(x[0]) ), d1)
        #return map(lambda x: strftime('%Y%m%d', time.strptime('%s %s %s' % (month_names.index(x[1])+1, x[0], year), "%m %d %Y")), d1)
        #print d1
        print holidays
        
        return holidays
        
    except:
        traceback.print_exc()
    



def get_HSI_last_trading_day(holidays, month, year):

    cal = HongKong()
    map(lambda x: cal.addHoliday(str2qdate(x)), holidays)    
    
    def deduce_last_trading_day(ld):
        #print ld
        ld = ld - 1
        #print '###' + str(ld)
        
        if cal.isHoliday(ld):
            return deduce_last_trading_day(ld)
        elif not cal.isBusinessDay(ld):
            return deduce_last_trading_day(ld)

        else:
            #print '---' + str(ld-1)
            
            return ld
 
    
    def deduce_last_business_day(ld):
        #print ld
        #ld = ld - 1
        #print '###' + str(ld)
        
        if cal.isHoliday(ld):
            return deduce_last_business_day(ld - 1)
        elif not cal.isBusinessDay(ld):
            return deduce_last_business_day(ld - 1)
        else:
            #print '---' + str(ld-1)
            
            return ld
    
    return qdate2str(deduce_last_trading_day(deduce_last_business_day(Date.endOfMonth(Date(1, month, year)))))
    
    
# QUANTLIB Period class usage:
# https://ipythonquant.wordpress.com/2015/04/04/a-brief-introduction-to-the-quantlib-in-python/
# check usage below:
    
    
# def get_HSI_expiry(year):
# 
#     month_names = [January,
#                     February,
#                     March,
#                     April,
#                     May,
#                     June,
#                     July,
#                     August,
#                     September,
#                     October,
#                     November,
#                     December,
#                     ]    
#     mm_dd = {}
#     # load calendar
#     holidays = ['20160101', '20160208', '20160209',  '20160210', '20160325', '20160326', '20160328', '20160404', '20160502', '20160514', '20160609', '20160701', '20160916', '20161001', '20161010', '20161226', '20161227']
#     chk = HongKong()
#     
#     
#     map(lambda x: chk.addHoliday(Date(int(x[6:8]), month_names[int(x[4:6])-1], int(x[0:4]))), holidays)
#     
#     
#     #print Date.todaysDate() - 1
#     #chk.addHoliday(Date(24, December, 2015))
#     def deduce_last_trading_day(ld):
#         # ld -=1
#         ld = ld - 1
#         
#         if chk.isHoliday(ld):
#             return deduce_last_trading_day(ld - 1)
#         elif not chk.isBusinessDay(ld):
#             return deduce_last_trading_day(ld - 1)
#         
#         print ld
#         return ld
#         
#     
#         
#     return map(lambda x: deduce_last_trading_day(Date.endOfMonth(Date(1, x, year) + Period("1m"))), range(1, 12))
#     #mm_dd = deduce_last_trading_day(Date.endOfMonth(Date.todaysDate())) 
#     #return mm_dd
    

if __name__ == '__main__':
    
    
#http://stackoverflow.com/questions/4891490/calculating-europeanoptionimpliedvolatility-in-quantlib-python
    
    # todaysDate = Date(10,August, 2015)
    # Settings.instance().evaluationDate = todaysDate
    # 
    # exercise = EuropeanExercise(Date(28,August,2015))
    # payoff = PlainVanillaPayoff(Option.Call, 25600.0)
    # option = EuropeanOption(payoff,exercise)
    # 
    # v = 0.19
    # S = QuoteHandle(SimpleQuote(24507.0))
    # r = YieldTermStructureHandle(FlatForward(0, TARGET(), 0.0005, Actual360()))
    # q = YieldTermStructureHandle(FlatForward(0, TARGET(), 0.0005, Actual360()))
    # sigma = BlackVolTermStructureHandle(BlackConstantVol(0, TARGET(), v, Actual360()))
    # process = BlackScholesMertonProcess(S,q,r,sigma)
    # 
    # 
    # im = option.impliedVolatility(85.0, process)
    # print im
    # 
    # engine = AnalyticEuropeanEngine(process)
    # option.setPricingEngine(engine)
    # print '%0.4f delta=%0.4f gamma=%0.4f theta=%0.4f vega=%0.4f ' % (option.NPV(), option.delta(), option.gamma(), option.theta() / 360, option.vega())
    
    
    # for i in range(24000, 27000, 200):
    #     results = cal_option(24189.0, i * 1.0, 'C', '20150804', '20150828', 0.0005, 0.0005, 0.19)
    #     results2 = cal_option(24189.0, i * 1.0, 'P', '20150804', '20150828', 0.0005, 0.0005, 0.19)
    #     print ('%d: '% i) + ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems()) + '|' +  ''.join ('%s=%0.4f, '%(k,v) for k, v in results2.iteritems())
    # for k, v in results.iteritems():
    #     print '%s= %0.4f' % (k, v)
    
    
    #spot 24119.0, X 25000, right: P, evaldate: 20150812, expiry: 20150828, rate: 0.0005, div: 0.0005, vol: 0.2000, premium: 334.0000
    #spot 24149.0, X 25200, right: P, evaldate: 20150812, expiry: 20150828, rate: 0.0005, div: 0.0005, vol: 0.2000, premium: 437.5000
    
    results = cal_option(22363.0, 22000, 'C', '20151201', '20151230', 0.00012, 0.0328, 0.198)
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
#     results = cal_option(23067.0, 22000, 'P', '20151018', '20151029', 0.0009, 0.0328, 0.2918)
#     npv1 = results['npv']
#     v1 = 0.2918
# 
#     
#     print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
#     results = cal_option(23067.0, 22000, 'C', '20151018', '20151029', 0.0009, 0.0328, 0.2918)
#     print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
# 
#     results = cal_option(23067.0, 22000, 'P', '20151018', '20151029', 0.0009, 0.0328, 0.2918*1.01)
#     print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
#     npv2 = results['npv']
#     v2 = v1 * 1.01
#     
#     print 'validating vega: (%0.2f - %0.2f) / (%0.4f - %0.4f) = %0.2f' % (npv2, npv1, v2, v1, (npv2-npv1)/ (v2 - v1))     
#     print 'validating gamma: (%0.2f - %0.2f) / (%0.4f - %0.4f) = %0.2f' % (npv2, npv1, v2, v1, (npv2-npv1)/ (v2 - v1))
#     
#     results = cal_option(23067.0, 22000, 'C', '20151018', '20151029', 0.0009, 0.0328, 0.2918*1.01)
#     print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())

    
 
    
    #results = cal_implvol(22363.0, 22000, 'C', '20151201', '20151230', 0.0328, 0.00012, 0.21055, 
    
        
    #print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())

# 
#     chk = HongKong()
#     chk.addHoliday(Date(24, December, 2015))
#     chk.addHoliday(Date(19, October, 2015))
#     chk.addHoliday(str2qdate('20151020'))
#     print chk.isBusinessDay(Date(24, December, 2015))
#     print chk.isBusinessDay(Date(19, October, 2015))
#     print chk.isEndOfMonth(Date(29, October, 2015))
#     print chk.isEndOfMonth(Date(30, October, 2015))
#     print chk.advance(Date(17, October, 2015), 1, 0)
#     print chk.advance(Date(17, October, 2015), 1, 1)
#     print chk.advance(Date(17, October, 2015), 1, 2)
    #print get_HSI_expiry(2016)
    
    holidays = get_hk_holidays(2016)

    
    
    month_names = [January,
                February,
                March,
                April,
                May,
                June,
                July,
                August,
                September,
                October,
                November,
                December,
                ] 
    for i in month_names:
        dd = get_HSI_last_trading_day(holidays, i, 2016)
        print dd
        