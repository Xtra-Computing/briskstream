#!/bin/bash

# this is for sigmod06-two equivalence

dirname=twoequivalence_results_$(date +%Y%m%d-%T)
mkdir $dirname

#set the print to 1 if you want to see the details of the results
#set the print to 0 if you DONOT want to see the details of the results

print=1 

echo "two equivalence: first attribute 10"
sase $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence.query $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence10.stream $print >>$dirname/te10.result


echo "two equivalence: first attribute 50"
sase $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence.query $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence50.stream $print >>$dirname/te50.result


echo "two equivalence: first attribute 100"
sase $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence.query $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence100.stream $print >>$dirname/te100.result


echo "two equivalence: first attribute 500"
sase $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence.query $SASE_HOME/tests/SimplePattern/TwoEquivalence/twoequivalence500.stream $print >>$dirname/te500.result






