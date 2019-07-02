#!/bin/bash

# This is for sigmod08 experiments

dirname=querytype_results_$(date +%Y%m%d-%T)
mkdir $dirname

#set the print to 1 if you want to see the details of the results
#set the print to 0 if you DONOT want to see the details of the results

print=1 

echo "s2-p1"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s2-p1.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts2-p1.result

echo "s2-p2"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s2-p2.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts2-p2.result

echo "s2-p3"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s2-p3.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts2-p3.result

echo "s3-p1"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s3-p1.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts3-p1.result

echo "s3-p2"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s3-p2.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts3-p2.result

echo "s3-p3"
sase $SASE_HOME/tests/KleeneClosure/QueryType/s3-p3.query $SASE_HOME/tests/KleeneClosure/QueryType/querytype.stream $print >>$dirname/qts3-p3.result