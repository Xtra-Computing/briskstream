#!/bin/bash
ocperf.py stat -e \
cpu_clk_unhalted.thread_p_any,\
dtlb_store_misses.stlb_hit,\
dtlb_store_misses.walk_duration,\
dtlb_load_misses.stlb_hit,\
dtlb_load_misses.walk_duration,\
icache.misses,\
idq.empty,\
idq_uops_not_delivered.core,\
ild_stall.iq_full,\
ild_stall.lcp,\
inst_retired.any,\
int_misc.recovery_cycles,\
itlb_misses.stlb_hit,\
itlb_misses.walk_duration,\
ld_blocks.store_forward,\
ld_blocks_partial.address_alias,\
uops_issued.any,\
uops_retired.core_stall_cycles,\
uops_retired.retire_slots,\
stalled-cycles-frontend -p $1 -o $2 -a sleep 30
