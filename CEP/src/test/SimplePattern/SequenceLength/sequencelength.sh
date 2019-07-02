#!/bin/bash

# this is for sigmod06-sequence length

dirname=sequencelength_results_$(date +%Y%m%d-%T)
mkdir $dirname
#set the print to 1 if you want to see the details of the results
#set the print to 0 if you DONOT want to see the details of the results

print=1 

echo "sequence length: 2"
sase $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength2.query $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength.stream $print >>$dirname/sl2.result


echo "sequence length: 3"
sase $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength3.query $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength.stream $print >>$dirname/sl3.result


echo "sequence length: 4"
sase $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength4.query $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength.stream $print >>$dirname/sl4.result


echo "sequence length: 5"
sase $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength5.query $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength.stream $print >>$dirname/sl5.result


echo "sequence length: 6"
sase $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength6.query $SASE_HOME/tests/SimplePattern/SequenceLength/sequencelength.stream $print >>$dirname/sl6.result

