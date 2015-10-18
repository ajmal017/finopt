# -*- coding: utf-8 -*-

from QuantLib import *



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

 

    return results
    
        


def str2qdate(yyyymmdd):
    months = [January, February, March, April, May, June, July, August, September, October,
                November, December]
    #print '%d%d%d'% (int(yyyymmdd[6:8]), int(yyyymmdd[4:6])-1 , int(yyyymmdd[0:4])) 
    return Date(int(yyyymmdd[6:8]), months[int(yyyymmdd[4:6])-1 ], int(yyyymmdd[0:4]))
    

def str2qopt_type(callput):
    if callput.upper() == 'C':
        return Option.Call
    return Option.Put

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
    
    results = cal_option(23067.0, 22000, 'P', '20151018', '20151030', 0.0009, 0.0328, 0.2918)
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
    results = cal_option(23067.0, 22000, 'P', '20151018', '20151029', 0.0009, 0.0328, 0.2918)
    npv1 = results['npv']
    v1 = 0.2918

    
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
    results = cal_option(23067.0, 22000, 'C', '20151018', '20151029', 0.0009, 0.0328, 0.2918)
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())

    results = cal_option(23067.0, 22000, 'P', '20151018', '20151029', 0.0009, 0.0328, 0.2918*1.01)
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())
    npv2 = results['npv']
    v2 = v1 * 1.01
    
    print 'validating vega: (%0.2f - %0.2f) / (%0.4f - %0.4f) = %0.2f' % (npv2, npv1, v2, v1, (npv2-npv1)/ (v2 - v1))     
    
    
    results = cal_option(23067.0, 22000, 'C', '20151018', '20151029', 0.0009, 0.0328, 0.2918*1.01)
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())

    
 
    
    results = cal_implvol(23067.0, 21000, 'P', '20151018', '20151030', 0.0009, 0.0328, 0.19, 14.5817)
    
        
    print ''.join ('%s=%0.4f, '%(k,v) for k, v in results.iteritems())


    chk = HongKong()
    chk.addHoliday(Date(24, December, 2015))
    chk.addHoliday(Date(19, October, 2015))
    chk.addHoliday(str2qdate('20151020'))
    print chk.isBusinessDay(Date(24, December, 2015))
    print chk.isBusinessDay(Date(19, October, 2015))
    print chk.isEndOfMonth(Date(29, October, 2015))
    print chk.isEndOfMonth(Date(30, October, 2015))
    print chk.advance(Date(17, October, 2015), 1, 0)
    print chk.advance(Date(17, October, 2015), 1, 1)
    print chk.advance(Date(17, October, 2015), 1, 2)