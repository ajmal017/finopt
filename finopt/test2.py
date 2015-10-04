import redis, json
from finopt.cep.redisQueue import RedisQueue
from numpy import *
import pylab
import ystockquote
from datetime import datetime
from scipy import stats

def f1():
    pall = set(rs.keys(pattern='PT_*'))
    pcall = pall.difference(rs.keys(pattern='PT*P'))
    pput = pall.difference(rs.keys(pattern='PT*C'))
    print pall
    print pcall
    
    
    max = lambda a,b: a if (a[1] > b[1]) else b
    min = lambda a,b: a if (a[1] < b[1]) else b
    
    
    a = [(x,json.loads(rs.get(x[3:]))['5002']) for x in pall]
    print sorted(a)
    
    
    # instrument with the largest +ve delta impact
    print reduce(max, a)
    
    # call instrument with the largest +ve delta impact
    print reduce(max, [(x,json.loads(rs.get(x[3:]))['5002']) for x in pcall ])

    # put instrument with the largest +ve delta impact
    print reduce(max, [(x,json.loads(rs.get(x[3:]))['5002']) for x in pput ])

    
    # instrument with the largest -ve delta impact
    print reduce(min,[(x,json.loads(rs.get(x[3:]))['5002']) for x in pall])


def f2():
    pall = set(rs.keys(pattern='PT_*'))
    s = '["symbol","right","avgcost","spotpx","pos","delta","theta","pos_delta","pos_theta","unreal_pl","last_updated"'
    
    def split_toks(x):
        pmap = json.loads(rs.get(x))
        print pmap
        gmap = json.loads(rs.get(x[3:]))
        print gmap
        s = '["%s","%s",%f,%f,%f,%f,%f,%f,%f,%f,"%s"],' % (x[3:], x[len(x)-1:], pmap['6001'], gmap['5006'], pmap['6002'],\
                                                                     gmap['5002'],gmap['5004'],\
                                                                     pmap['6005'],pmap['6006'],pmap['6008'],pmap['last_updated'])
        return s                                                          
        
    return ''.join (split_toks( x ) for x in pall) 


def extrapolate(ric):

# data to fit

    def get_hist_data():
        l = ystockquote.get_historical_prices(ric, '20150101', '20150916')
        #print l
        xd = [float(x[4]) for x in l[1:]]
        #yd = [datetime.strptime(y[0], '%Y-%m-%d') for y in l[1:]]
        yd = [y for y in range(len(l[1:]))]
        xd.reverse()
        return xd, yd
        

    yd, xd = get_hist_data()
    
    for d in range(30, len(yd[1:]), 10):
        x = xd[1:d]
        y = yd[1:d]
    #     x = random.rand(6)
    #     y = random.rand(6)
        
        # fit the data with a 4th degree polynomial
        z4 = polyfit(x, y, 4) 
        p4 = poly1d(z4) # construct the polynomial 
        
        z5 = polyfit(x, y, 5)
        p5 = poly1d(z5)
        
        xx = linspace(0, 350, 100)
        pylab.plot(x, y, 'o', xx, p4(xx),'-g', xx, p5(xx),'-b', xd, yd, '-r')
        pylab.legend(['%s to fit' % ric, '4th degree poly', '5th degree poly'])
        #pylab.axis([0,160,0,10])
        
        pylab.axis([0,len(xd[1:]),min(yd) *.95,max(yd) * 1.1])
     
        sr, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), yd[d:d+5])
        s4, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), [p4(i) for i in range(d,d+5)])
        s5, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), [p5(i) for i in range(d,d+5)])
        print 'sr(%s):%f   s4(%s):%f    s5(%s):%f' % ('up' if sr > 0 else 'down', sr, 'up' if s4 > 0 else 'down', s4, 'up' if s5 > 0 else 'down', s5 )
        
        for i in range(d, d+5):
            print '%f, %f, %f' % (yd[i], p4(i), p5(i))
        #pylab.show()
        pylab.savefig('./data/extrapolation/%s-%d.png' % (ric, d))
        pylab.close()


def extrapolate2(ric):

# data to fit

    def get_hist_data():
        l = ystockquote.get_historical_prices(ric, '20150101', '20150922')
        #print l
        xd = [float(x[4]) for x in l[1:]]
        #yd = [datetime.strptime(y[0], '%Y-%m-%d') for y in l[1:]]
        yd = [y for y in range(len(l[1:]))]
        xd.reverse()
        return xd, yd
        
        
    # x1, y1 are a subset of points of size d taken from (xr, yr) 
    # these are points to be fed into the extrapolation function
    # x2 provides the line space to pass to p4 and p5 -> p4(1..x2) and p5(1..x2)
    # xr, yr contains the full set of data (each data point on y axis represents 1 day)
    
    def detect_trend(x1, y1, xr, yr, d):
        z4 = polyfit(x1, y1, 4) 
        p4 = poly1d(z4) # construct the polynomial 
        #print y1
        
        z5 = polyfit(x1, y1, 5)
        p5 = poly1d(z5)
        
        extrap_y_max_limit = len(x1) * 2  # 360 days
        x2 = linspace(0, extrap_y_max_limit, 100) # 0, 160, 100 means 0 - 160 with 100 data points in between
        pylab.plot(x1, y1, 'o', x2, p4(x2),'-g', x2, p5(x2),'-b', xr, yr, '-r')
        pylab.legend(['%s to fit' % ric, '4th degree poly', '5th degree poly'])
        #pylab.axis([0,160,0,10])
        
        pylab.axis([0,len(xr[1:])*1.1, min(yr) *.95,max(yr) * 1.1])  # first pair tells the x axis boundary, 2nd pair y axis boundary 
        
        # compute the slopes of each set of data points
        # sr - slope real contains the slope computed from real data points from d to d+5 days
        # s4 - slope extrapolated by applying 4th degree polynomial
        sr, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), yr[d:d+5])
        s4, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), [p4(i) for i in range(d,d+5)])
        s5, intercept, r_value, p_value, std_err = stats.linregress(range(0,5), [p5(i) for i in range(d,d+5)])
        
        # a 3-item tuple, a value of 1 in item 1 means a correct guess, same for item 2, item 3 is just an accumulator counter used to track the number of runs  
        rc = (1 if (sr > 0.0 and s4 > 0.0) or (sr < 0.0 and s4 < 0.0) else 0.0, 1.0 if (sr > 0.0 and s5 > 0.0) or (sr < 0.0 and s5 < 0.0) else 0.0, 1.0)
        
#         print 'sr(%s):%f   s4(%s):%f    s5(%s):%f' % ('up' if sr > 0 else 'down', sr, 'up' if s4 > 0 else 'down', s4, 'up' if s5 > 0 else 'down', s5 )        
#         for i in range(d, d+5):
#             print '%f, %f, %f' % (yr[i], p4(i), p5(i))
        #pylab.show()
        pylab.savefig('./data/extrapolation/%s-%d.png' % (ric, d))
        # clear memory
        pylab.close()
        return rc



    yd, xd = get_hist_data()
    
    # start with 30 points, then 40, 50, 60...
    rcs = []
    for d in range(30, len(yd[1:])-10, 10):
        x = xd[1:d]
        y = yd[1:d]
        rcs.append(detect_trend(x, y, xd, yd, d))
    #print rcs
    score = reduce(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], (x[0]+y[0]) / (x[2]+y[2]), (x[1]+y[1]) / (x[2]+y[2])), rcs)
    print "for a total of %d iterations, p4 guessed correctly the direction of %s movement %d times (%0.4f) whereas p5 guessed correctly %d times (%0.4f)."\
             % (score[2], ric, score[0], score[3], score[1], score[4])
    
    return (ric, score)



        
def mark6():
    
    
    f = open('/home/larry/scribble/mark6.csv')
    l = f.readlines()
    n = map(lambda x: (x.split(',')), l)


    yy=[]
    pp=[]
    for i in range(1,8):
        yy.append( map(lambda x: x, [int(c[i]) for c in n]) )
        xx = range(1, len(yy[i-1])+1)
        print yy[i-1], xx
        z5 = polyfit(xx, yy[i-1], 4)
        pp.append(poly1d(z5))
#     p1 = map(lambda x: x, [c[1] for c in n])
#     p2 = map(lambda x: x, [c[2] for c in n])
#     p3 = map(lambda x: x, [c[3] for c in n])
#     p4 = map(lambda x: x, [c[4] for c in n])
#     p5 = map(lambda x: x, [c[5] for c in n])
#     p6 = map(lambda x: x, [c[6] for c in n])
#     p7 = map(lambda x: x, [c[7] for c in n])   
    print pp[0], yy[0]
    for i in range(7):
        print 'num: %d' % pp[i](31)       
    xxx = linspace(0, 60, 100)
    pylab.plot(xx, yy[0], 'o', xxx, pp[0](xxx),'-g')  
    pylab.plot(xx, yy[1], 'o', xxx, pp[1](xxx),'-r')
    pylab.plot(xx, yy[2], 'o', xxx, pp[2](xxx),'-b')  
    pylab.plot(xx, yy[3], 'o', xxx, pp[3](xxx),'-c')
    pylab.plot(xx, yy[4], 'o', xxx, pp[4](xxx),'-b')  
    pylab.plot(xx, yy[5], 'o', xxx, pp[5](xxx),'-p')
    
    pylab.axis([0,50, 0,50])
    pylab.show()


def analyze_all(fn):
    f = open(fn)
    l = f.readlines()
    rics = ['%s.HK' % s.strip('\n') for s in l]
    return rics


def analyze():
    f = open ('/home/larry-13.04/workspace/finopt/data/extrapolation/stocks-extrapolation-results.txt')
    l = f.readlines()
    ll = eval(l[0])
    dd = map (lambda x: (x[0], x[1][3]) if x[1][3] > 0.6 else None, ll)
    ee = filter(lambda x: x <> None, dd)
    print '\n'.join('%s %2f' % (x[0], x[1]) for x in ee)

    
    
if __name__ == '__main__':
#     rs = redis.Redis('localhost', 6379,3)
#     
#     s = f2()
#     q = RedisQueue('test', host='localhost', port=6379, db=3)
#     #[q.put(item) for item in s.split(',')]
#     
#     while not q.empty():
#         q.get()
#     [q.put(item) for item in range(1,100)]
#     print q.qsize()
#     #print q.qsize()
#     print q.peek()
#     print q.peek(50)
    #stk = ['1398.HK', '0992.HK', '0787.HK', 'DUG', 'USO']
    #stk = ['0700.HK', '0787.HK'] #,'0941.HK',  '2822.HK', '2823.HK', '0939.HK', '2318.HK', '1299.HK', '3988.HK', '1398.HK']
#     rics = analyze_all('./data/hkex-stock-list.txt')
# 
#     st = []
#     
#     for s in rics[185:]:
#         try:
#         
#             st.append(extrapolate2(s))
#         except:
#             print 'error'
#             continue
#             
#     print st
#    mark6()

#    analyze()
    
    
    


    
    