#!/bin/bash
#./run_app_storm_ts.sh $1
#./run_app_storm_configureTasks.sh $1
run_app_storm_IC.sh $1
run_app_storm_OC.sh $1
run_app_storm_window.sh $1
run_app_storm_ss.sh $1
