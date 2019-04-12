#!/bin/bash

# This script file is used to run Q2.
dirname=Q2_results_$(date +%Y%m%d-%T)
mkdir $dirname


echo "Q2"
sase $SASE_HOME/examples/Q2/Q2.query $SASE_HOME/examples/Q2/Q2.stream 1  >>$dirname/Q2.result