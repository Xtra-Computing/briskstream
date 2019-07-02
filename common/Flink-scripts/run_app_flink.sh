#!/bin/bash
run_app_flink_IC.sh $1
run_app_flink_OC.sh $1
run_app_flink_window.sh $1
run_app_flink_ss.sh $1