#!/bin/bash

# this is for sigmod06-time window
dirname=timewindow_results_$(date +%Y%m%d-%T)
mkdir $dirname

#set the print to 1 if you want to see the details of the results
#set the print to 0 if you DONOT want to see the details of the results

print=1 

echo "time window: 10k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow10k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow10k.stream $print >>$dirname/tw10k.result



echo "time window: 20k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow20k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow20k.stream $print >>$dirname/tw20k.result


echo "time window: 40k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow40k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow40k.stream $print >>$dirname/tw40k.result


echo "time window: 60k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow60k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow60k.stream $print >>$dirname/tw60k.result


echo "time window: 80k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow80k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow80k.stream $print >>$dirname/tw80k.result


echo "time window: 100k"
sase $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow100k.query $SASE_HOME/tests/SimplePattern/TimeWindow/timewindow100k.stream $print >>$dirname/tw100k.result