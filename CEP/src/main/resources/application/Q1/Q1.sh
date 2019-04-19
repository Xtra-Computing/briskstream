#!/bin/bash

# This script file is used to run Q1.
dirname=Q1_results_$(date +%Y%m%d-%T)
mkdir $dirname


echo "Q1"
sase $SASE_HOME/examples/Q1/Q1.query $SASE_HOME/examples/Q1/Q1.stream 1  >>$dirname/Q1.result