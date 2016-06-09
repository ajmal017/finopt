from sklearn.naive_bayes import BernoulliNB
import numpy as np
import finopt.ystockquote as yq
import datetime
from dateutil import rrule
import itertools

def weather_play():
    
    # implementing the example in the blog link below
    # http://www.analyticsvidhya.com/blog/2015/09/naive-bayes-explained/
    # each vector in x represents a predictor of type 'weather' with
    # attributes = ['sunny', 'overcast', 'rainy']
    # the label / class in y are ['NO', 'YES'] or 0,1
    
    # using Bernoulli because the vectors are in binary 
    
    x= np.array([[1,0,0],[1,0,0],[1,0,0],[1,0,0],
                [0,1,0],[0,1,0],[0,1,0],[0,1,0],[0,1,0],
                [0,0,1],[0,0,1],[0,0,1],[0,0,1],[0,0,1]])
                
    y = np.array([1,1,1,1,0,0,0,1,1,0,0,1,1,1])
    
    model = BernoulliNB()
    model.fit(x,y)
    predicted = model.predict([[0,0,1],[1,0,0]])
    print predicted
    print model.predict_proba([[0,0,1],[1,0,0],[0,1,0]])
    print model.feature_count_



def str2datetime(yyyymmdd):
    #print '%d%d%d'% (int(yyyymmdd[6:8]), int(yyyymmdd[4:6])-1 , int(yyyymmdd[0:4])) 
    return datetime.datetime(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


def ystr2datetime(yyyymmdd):
    #print '%d%d%d'% (int(yyyymmdd[6:8]), int(yyyymmdd[4:6])-1 , int(yyyymmdd[0:4])) 
    return datetime.datetime(int(yyyymmdd[0:4]), int(yyyymmdd[5:7]), int(yyyymmdd[8:10]))

def datetime2ystr(dt):
    return '{:%Y-%m-%d}'.format(dt)

def ewh_hsi(rs):

    def daily_change(code, frdate, todate, base, numerator):
        e0 = yq.get_historical_prices(code, frdate, todate)
        print e0
        e1 = e0[1:]
        e2 = e0[2:]
        
        e3 = map(lambda i: (e2[i][0], 
                            1 if (float(e2[i][numerator]) - float(e1[i][base])) / float(e1[i][base]) > 0 else 0,
                            e2[i][numerator], e1[i][base]
                            ), 
                            [i for i in range(len(e2))])
        return e3
    
    idx = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Clos']
    EWH = daily_change('^DJI', '20150901', '20160330', idx.index('Adj Clos'), idx.index('Adj Clos'))
    #EWH = EWH[:20]
    # 1 if opens high and 0 otherwise
    HSI = daily_change('^HSI', '20150901', '20160330', idx.index('Open'), idx.index('Adj Clos'))
    #HSI = HSI[:20]
    print len(EWH), ''.join('%s,' % x[0] for x in EWH)
    print len(HSI), ''.join('%s,' % x[0] for x in HSI)
    HSI_dates = map(lambda x: x[0], HSI)
    # filter EWH entries for which a record has a corresponding next trade record in HSI
    # example, EWH trade date 2016-02-29 the corresponding record for HSI is 2016-03-01
    EWH_filtered = filter(lambda x: datetime2ystr(rs.after(ystr2datetime(x[0]))) in HSI_dates,EWH)
    print len(EWH_filtered),  EWH_filtered
    hsi_ewh = map(lambda x:(HSI[HSI_dates.index(
                                            datetime2ystr(rs.after(ystr2datetime(x[0]))))
                                            ][1], x[1]), EWH_filtered)
    
    xx = np.array(map(lambda x: [x[1], 0], hsi_ewh))
    yy = np.array(map(lambda x: x[0], hsi_ewh))
    
    model = BernoulliNB()
    model.fit(xx,yy)
    predicted = model.predict([[0,0], [1,0]])
    print predicted
    print model.predict_proba([[0,0], [1,0]])
    print model.feature_count_    
    
    
def cartesian_product(a, b):
    return [[a0,b0] for a0 in a for b0 in b]
    
def permutations(size):
    #http://thomas-cokelaer.info/blog/2012/11/how-do-use-itertools-in-python-to-build-permutation-or-combination/
    return list(itertools.product([0,1], repeat=size))

def predict(rs):
    
    def daily_change(code, frdate, todate, base, numerator):
        # compute the next day price change % and return a new binary series where 
        # 1 - means UP
        # 0 - means DOWN
        # normailly this is calculated as (price of today - price of yesterday) / price of yesterday
        # price type can be specified using the 'base' and 'numerator' parameters
        
        e0 = yq.get_historical_prices(code, frdate, todate)
        print e0
        e1 = e0[1:]
        e2 = e0[2:]
        
        e3 = map(lambda i: (e2[i][0], 
                            1 if (float(e2[i][numerator]) - float(e1[i][base])) / float(e1[i][base]) > 0 else 0,
                            e2[i][numerator], e1[i][base],                            
                            (float(e2[i][numerator]) - float(e1[i][base])) / float(e1[i][base])
                            ),
                            [i for i in range(len(e2))])
        return e3
   
    def save_lf_series(name, series):
        now = datetime.datetime.now().strftime('%Y%m%d%H%M')
        f = open('%s/%s-%s' % ('../dat', name, now), 'w')
        f.write(''.join('%s %s,' % (x[0], x[1]) for x in series))
        f.close()
    
    def lbl_predictor_parse(c_stock, f_stock, frdate, todate):
    
        idx = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Clos']
        feature = daily_change(f_stock, frdate, todate, idx.index('Adj Clos'), idx.index('Adj Clos'))

    
        label = daily_change(c_stock, frdate, todate, idx.index('Open'), idx.index('Adj Clos'))
        #HSI = HSI[:20]
        print 'F: [%s] Num elements: %d ' % (f_stock, len(feature)), ''.join('(%s,%d,%0.4f), ' % (x[0],x[1],x[4]) for x in feature)
        print 'L: [%s] Num elements: %d ' % (c_stock, len(label)), ''.join('(%s,%d,%0.4f), ' % (x[0],x[1],x[4])  for x in label)
        
        # extract all the label dates
        label_trade_dates = map(lambda x: x[0], label)
        # filter feature series -  
        # example, for a record with trade date (T) 2016-02-29, expect to find a label record with date = T+1
        # if a match in the lable series couldn't be found, drop the feature record
        #
        # logic:
        # for each record in feature
        #     determine the next business date of "label" given the feature record's date
        #     if found, retrain, else, drop
        feature_filtered = filter(lambda x: datetime2ystr(rs.after(ystr2datetime(x[0]))) in label_trade_dates,feature)
        print 'Filtered F:[%s] Num elements: %d ' % (f_stock, len(feature_filtered)),  feature_filtered
        #
        # generate a labeledPoint (label, feature)
        label_feature = map(lambda x:(label[label_trade_dates.index(
                                                datetime2ystr(rs.after(ystr2datetime(x[0]))))
                                                ][1], x[1]), feature_filtered)
        print 'Matched Series [%s:%s] %s' % (c_stock, f_stock, ''.join('(%s,%s),' % (x[0], x[1]) for x in label_feature))
        
        save_lf_series('%s_%s' % (c_stock,f_stock), label_feature)
        
        return label_feature
    

        
    
    #features_config = {'cstock': '^HSI', 'fstocks': ['^DJI', '^FCHI', '^FVX', '^FTSE','VNQ','QQQ','GOOG','BAC'], 'date_range': ['20150901', '20160330']}
    features_config = {'cstock': '^HSI', 'fstocks': ['^DJI', 'EUR=X', 'JPY=X'], 'date_range': ['20150901', '20160330']}
    lf = []
    for fs in features_config['fstocks']:
        lf.append(lbl_predictor_parse(features_config['cstock'], fs, features_config['date_range'][0], features_config['date_range'][1]))
                  
#     lf1 = lbl_predictor_parse('^HSI', '^DJI', '20150901', '20160325')
#     lf2 = lbl_predictor_parse('^HSI', '^FTSE', '20150901', '20160325')
#     lf3 = lbl_predictor_parse('^HSI', '^HSCE', '20150901', '20160325')
#     xx1 = np.array(map(lambda x: [x[1], 0,    0], lf1))
#     xx2 = np.array(map(lambda x: [0   , x[1] ,0], lf2))
#     xx3 = np.array(map(lambda x: [0   , 0, x[1]], lf3))
#     xx = np.concatenate((xx1, xx2, xx3))
#     #print xx
# #     yy = np.array(map(lambda x: x[0], lf1+lf2+lf3))
#     model = BernoulliNB()
#     model.fit(xx,yy)
#     scenarios = [[0,0,0], [1,1,1],[0,0,1],[0,1,1],[1,0,0],[1,1,0]]
#     predicted = model.predict(scenarios)
#     print predicted
#     print model.predict_proba(scenarios)
#     print model.feature_count_     

    # build vector
    #[DJI, FTSE, HSCE]
    points_sp = []
    points_sk = []
    for i in range(len(lf)):
        
        def spark_friendly(v):
            # init a bunch of zeros [0,0,...]
            point = [0] * len(lf)
            # set the value at column i of the vector
            point[i] = v[1] 
            #print 'spark label:%s feature#:%d' %  (v[0], i),  point
            # retrun  a tuple of label, feature
            return (v[0], point)
        
        def sklearn_friendly(v):
            point = [0] * len(lf)
            point[i] = v[1] 
            #print 'sklearn label:%s feature#:%d' %  (v[0], i),  point
            return point
        #print 'len: ' , len(lf[i])
        points_sp.append(map(spark_friendly , lf[i]))
        points_sk.append(np.array(map(sklearn_friendly, lf[i])))
        
    #
    # format  [[(1, [1, 0, 0]), (1, [1, 0, 0])], [(0, [0, 0, 0]),...]] 
    def save_labelled_points(name, pt):
        now = datetime.datetime.now().strftime('%Y%m%d%H%M')
        now = ''
        f = open('%s/%s-%s' % ('../dat', name, now), 'w')
        
        for i in range(len(points_sp)):
            for j in range(len(points_sp[i])):
                print '%s,%s' % (points_sp[i][j][0], ' '.join('%d' % s for s in points_sp[i][j][1]))
                f.write('%s,%s\n' % (points_sp[i][j][0], ' '.join('%d' % s for s in points_sp[i][j][1])))
                
        f.close()                
        
    print "For pyspark LabeledPoint format: ", points_sp
    save_labelled_points('%s-%s' % (features_config['cstock'], '_'.join(s for s in features_config['fstocks'])), points_sp)
    
    points_sk = np.concatenate((points_sk))
    print "For sklearn numpy format:\n ", points_sk
    #print np.concatenate((points))        
  
    #print len(lf[0]+lf[1]+lf[2]), len(reduce(lambda x,y:x+y, lf))  , len(points_sp)
    yy = np.array(map(lambda x: x[0], reduce(lambda x,y:x+y, lf)))
    model = BernoulliNB()
    model.fit(points_sk,yy)
    #scenarios = [[0,0,0], [1,1,1],[0,0,1],[0,1,1],[1,0,0],[1,1,0]]
    num_features= len(points_sk[0])
    scenarios = permutations(num_features)
    
    predicted = model.predict(scenarios)
    print predicted, scenarios
    predicted_proba = model.predict_proba(scenarios)
    print predicted_proba
    print model.feature_count_   

    print '************** SUMMARY REPORT **************'
    print 'Likelihood (%s) GIVEN (%s)' % (features_config['cstock'], ', '.join(s for s in features_config['fstocks']))
    print 'Expected\t\tResult\t\tScneario'
    
    for i in range(len(predicted)):
        print '%s:\t\t %s\t\t%s' % ('UP' if predicted[i] == 1 else 'DOWN', scenarios[i], predicted_proba[i]) 
        
def test():
    #[DJI, FTSE, HSCE]
    points = []
    for i in range(3):
        
        def f1(v):
            
            point = [0] * len(range(3))
            point[i] = v
            print i, point
            return point
        
        points.append(np.array(map(f1 , [7,8,9])))
            

    
    print points
    print np.concatenate((points))
  
    
def set_biz_calendar():    
    #hk holidays
    holidays = [str2datetime('20150903'),
                str2datetime('20150928'),
                str2datetime('20151225'),
                str2datetime('20151226'),
                str2datetime('20150701'),
                str2datetime('20160101'),
                str2datetime('20160208'),
                str2datetime('20160209'),
                str2datetime('20160210'),
                str2datetime('20160325'),
                str2datetime('20160326'),
                str2datetime('20160328'),
                str2datetime('20160404'),
                str2datetime('20160502')]
    
    r = rrule.rrule(rrule.DAILY, 
                    byweekday=[rrule.MO, rrule.TU, rrule.WE, rrule.TH, rrule.FR],
                    dtstart = str2datetime('20151201'))
    rs = rrule.rruleset()
    rs.rrule(r)
    for exdate in holidays:
        rs.exdate(exdate)
    
    
    return rs
    
    #print np.array(s1)
if __name__ == '__main__':
    #weather_play()
    #test()
    
    #
    # 
    # Model: 
    # 
    # What is the likelihood of HSI opens high given
    # the dow jones or some other indices closed high on 
    # the previous trading day?
    #
    rs = set_biz_calendar()
    print ''.join('%s,\n' % rs[i] for i in range(5)), rs.after(str2datetime('20160324')),\
                                                           datetime2ystr(rs.after(str2datetime('20160324')))
     
    #ewh_hsi(rs)
    predict(rs)