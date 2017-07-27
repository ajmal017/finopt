import math

def jsfile(dp, fout, title):
    
    htmltext = """
    <html>
  <head>
    <!--Load the AJAX API-->
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">

      // Load the Visualization API and the piechart package.
      google.load('visualization', '1.0', {'packages':['corechart']});

      // Set a callback to run when the Google Visualization API is loaded.
      google.setOnLoadCallback(drawChart);

      // Callback that creates and populates a data table,
      // instantiates the pie chart, passes in the data and
      // draws it.
      function drawChart() {
var data = google.visualization.arrayToDataTable(
%s
);

        var options = {
          title: '%s',
          legend: { position: 'top', maxLines: 2 },
 
    colors: ['#5C3292', '#1A8763', '#871B47', '#999999', '#5CFF92'],
    interpolateNulls: false,          
        };

        var chart = new google.visualization.Histogram(document.getElementById('chart_div'));
        chart.draw(data, options);
        
        
        
      }
    </script>
  </head>

  <body>
    <!--Div that will hold the pie chart-->
    <div id="chart_div"></div>
  </body>
</html>
    
    
    """

    htmltext = htmltext % (dp, title)
    f = open(fout, 'w')
    f.write(htmltext)
    f.close()
    #print htmltext

def jsfile2(dp, fout, title):
    
    htmltext = """
    <html>
  <head>
    <!--Load the AJAX API-->
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">

      // Load the Visualization API and the piechart package.
      google.load('visualization', '1.0', {'packages':['corechart']});

      // Set a callback to run when the Google Visualization API is loaded.
      google.setOnLoadCallback(drawChart);

      // Callback that creates and populates a data table,
      // instantiates the pie chart, passes in the data and
      // draws it.
      function drawChart() {
var data = google.visualization.arrayToDataTable(
%s
);

        var options = {
          title: '%s',
          legend: { position: 'top', maxLines: 2 },
 
    colors: ['#5C3292', '#1A8763', '#871B47', '#999999', '#5CFF92'],
    interpolateNulls: false,          
        };

        var chart = new google.visualization.Histogram(document.getElementById('chart_div'));
        chart.draw(data, options);
        
        
        
      }
    </script>
  </head>

  <body>
    <!--Div that will hold the pie chart-->
      <div id="chart_div"></div>
  </body>
</html>
    
    
    """

    htmltext = htmltext % (dp, title)
    f = open(fout, 'w')
    f.write(htmltext)
    f.close()
    #print htmltext


def jsfile3(fout, jschunk, htmldiv):
    
    htmltext = """
    <html>
  <head>
    <!--Load the AJAX API-->
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">

      // Load the Visualization API and the piechart package.
      google.load('visualization', '1.0', {'packages':['corechart']});

      // Set a callback to run when the Google Visualization API is loaded.
      google.setOnLoadCallback(drawChart);

      // Callback that creates and populates a data table,
      // instantiates the pie chart, passes in the data and
      // draws it.
      function drawChart() {

    %s    
        
             
        
      }
    </script>
  </head>

  <body>
    <!--Div that will hold the pie chart-->
    %s
  </body>
</html>
    
    
    """

    htmltext = htmltext % (jschunk, htmldiv)
    f = open(fout, 'w')
    f.write(htmltext)
    f.close()
    #print htmltext


def format_weekly_data_points3(infile, outfile, title):


    jstext = """

var data%s = google.visualization.arrayToDataTable(
%s
);

        var options = {
          title: '%s',
          legend: { position: 'top', maxLines: 2 },
 
    colors: ['#5C3292', '#1A8763', '#871B47', '#999999', '#5CFF92'],
    interpolateNulls: false,          
        };

        var chart%s = new google.visualization.Histogram(document.getElementById('chart_div%s'));
        chart%s.draw(data%s, options);
        
        

    """
    
    f = open(infile)
    points_series = {}
    cnt = 0
    for l in f.readlines():
        l = l.strip("\n").split(',')
        
        # to limit the data points to a partcular year range, add the condition
        # l[2][0:4] not in ['2014', '2015', '2013']:
        
        if math.fabs(float(l[1])) > 0.035 or l[2][0:4] not in ['2014', '2015', '2013']:
            continue
        else:
            if l[2][5:7] not in points_series:
                points_series[l[2][5:7]]= []
                
            if l[0] == '1':
                points_series[l[2][5:7]].append( "[%s, null,null,null,null]," % l[1])
            elif l[0] == '2':
                points_series[l[2][5:7]].append( "[null, %s, null,null,null]," % l[1])
            elif l[0] == '3':
                points_series[l[2][5:7]].append( "[null,null,%s, null,null]," % l[1])
            elif l[0] == '4':
                points_series[l[2][5:7]].append( "[null,null,null,%s, null]," % l[1])
            elif l[0] == '5':
                points_series[l[2][5:7]].append( "[null,null,null,null,%s ]," % l[1])

#         cnt = cnt + 1
#         if cnt > 50:
#             break

            
    s = '[["M", "T", "W", "R", "F"],'
    jstexts = {}
    jschunk = ''
    htmldiv = ''

    for month, magnitudes in points_series.iteritems():
    

        s = '[["M", "T", "W", "R", "F"],'
        s = s + ''.join(e for e in magnitudes)
        s = s[:len(s)-1] + ']'
        jstexts[month] = jstext
        jstexts[month] = jstexts[month] % (month, s, "HKCE Open/Previous day fluctuation by month:" + month, month, month, month, month)
        print month, len(magnitudes) -1
        jschunk = jschunk + jstexts[month]
        
    
    for month in sorted(points_series.keys()):
        htmldiv = htmldiv + '<div id="chart_div%s"></div>' % month

    #print jschunk
    print htmldiv
    jsfile3(outfile, jschunk, htmldiv)
    

    

def format_weekly_data_points2(infile, outfile, title):
    # this routine formats data into a tabular format of 5 columns
    # each column represents a series of a weekday with monday starts at 1
    # and tuesday starts at 2 
    # the value at each column is the % change in HSI on the open vs previous close
    # the objective is to see whether there exists a pattern with mondays 
    # experience more fluctations when market opens
    f = open(infile)
    points = []
    for l in f.readlines():
        l = l.strip("\n").split(',')
        if math.fabs(float(l[1])) > 0.035 or l[2][5:7] <> '10':
            continue
        else:
            if l[0] == '1':
                points.append( "[%s, null,null,null,null]," % l[1])
            elif l[0] == '2':
                points.append( "[null, %s, null,null,null]," % l[1])
            elif l[0] == '3':
                points.append( "[null,null,%s, null,null]," % l[1])
            elif l[0] == '4':
                points.append( "[null,null,null,%s, null]," % l[1])
            elif l[0] == '5':
                points.append( "[null,null,null,null,%s ]," % l[1])

            
    s = '[["M", "T", "W", "R", "F"],'

    s = s + ''.join(e for e in points)
    s = s[:len(s)-1] + ']'
    
    print len(points) -1
    jsfile(s, outfile, title)


def format_data_points(infile, outfile, title):
    f = open(infile)
    points = []
    for l in f.readlines():
        l = l.strip("\n").split(',')
        points.append( "['%s', %s]," % (l[0], l[1]))
    s = '[["daily change", "Num Days"],'

    s = s + ''.join(e for e in points)
    s = s[:len(s)-1] + ']'
    print s
    print len(points) -1
    jsfile(s, outfile, title)
    
if __name__ == '__main__':
    #format_data_points("data/hsi-monday-change.csv", "data/hsi-monday-change.html", "monday open change magnitude")
    #format_weekly_data_points2("data/hsi-weekday-change.csv", "data/hsi-weekday-change.html", "weekday open change magnitude")
    #format_weekly_data_points2("data/hsi-weekday-change-over-5-years.csv", "data/hsi-weekday-change-over-5-years.html", "weekday open change magnitude")
    #format_weekly_data_points3("data/hsi-weekday-change-over-5-years.csv", "data/hsi-weekday-change-over-5-years.html", "weekday open change magnitude")
    format_weekly_data_points3("data/hkce-weekday-change-over-5-years.csv", "data/hkce-weekday-change-over-5-years.html", "weekday open change magnitude")
    