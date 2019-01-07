#!/bin/bash
ocperf.py stat -e \
cpu_clk_unhalted.thread_p_any,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.any_response,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.local_dram,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.remote_hitm_hit_forward,\
idq_uops_not_delivered.core,\
dtlb_load_misses.stlb_hit,\
dtlb_load_misses.miss_causes_a_walk,\
icache.misses,\
mem_load_uops_retired.l2_hit,\
mem_load_uops_retired.llc_hit,\
int_misc.recovery_cycles,\
itlb_misses.stlb_hit,\
itlb_misses.miss_causes_a_walk,\
uops_retired.all,\
uops_retired.retire_slots,\
uops_issued.any -o $1 -a -p $2 -D 10000 -S
