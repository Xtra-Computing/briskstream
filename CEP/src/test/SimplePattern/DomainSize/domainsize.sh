#!/bin/bash

# this is for domain size experiments


dirname=domainsize_results_$(date +%Y%m%d-%T)
mkdir $dirname

#set the print to 1 if you want to see the details of the results
#set the print to 0 if you DONOT want to see the details of the results

print=1 

echo "domainsize 100"
sase $SASE_HOME/tests/SimplePattern/DomainSize/domainsize.query $SASE_HOME/tests/SimplePattern/DomainSize/domainsize100.stream $print  >>$dirname/ds100.result


echo "domainsize 500"
sase $SASE_HOME/tests/SimplePattern/DomainSize/domainsize.query $SASE_HOME/tests/SimplePattern/DomainSize/domainsize500.stream $print >>$dirname/ds500.result

echo "domainsize 1000"
sase $SASE_HOME/tests/SimplePattern/DomainSize/domainsize.query $SASE_HOME/tests/SimplePattern/DomainSize/domainsize1000.stream $print >>$dirname/ds1000.result


echo "domainsize 5000"
sase $SASE_HOME/tests/SimplePattern/DomainSize/domainsize.query $SASE_HOME/tests/SimplePattern/DomainSize/domainsize5000.stream $print >>$dirname/ds5000.result


echo "domainsize 1000"
sase $SASE_HOME/tests/SimplePattern/DomainSize/domainsize.query $SASE_HOME/tests/SimplePattern/DomainSize/domainsize10000.stream $print >>$dirname/ds10000.result


