#!/bin/bash
#set -x
JAR_PATH="$HOME/NUMA-streamBenchmarks/StormBenchmarks/target/Storm-1.0-SNAPSHOT-jar-with-dependencies.jar"
Common_PATH="$HOME/results/compatibility"
MAIN="applications.BriskRunner"
JIT=0
x_start=0
x_start_end=0
#SINK_CLASS="applications.general.sink.NullSink_warmup"
SINK_CLASS="applications.general.sink.NullSink_stable"
SPOUT_CLASS="applications.general.spout.KafkaSpout"
cores="$(getconf _NPROCESSORS_ONLN)"
down=$cores
up=$cores
let vm=$cores*12
echo "Total VM: $vm, we use 12 over 16 GB memory for each core, some are reserved for O.S"
xmx=Xmx$vm\g
xms=Xms$vm\g
searchdir="--search-dir all:rp=/home/compatibility/compatibility-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=/home/compatibility/Documents/apache-compatibility-1.0.1/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=/home/tony/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"

if [ $vm -gt 100 ] ; then
	co1="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages -XX:LargePageSizeInBytes=2m"
	co2="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -agentlib:jprof=gencalib,logpath=/home/compatibility/executionNode"
	co3="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
else
	co1="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseLargePages -XX:LargePageSizeInBytes=2m"
	co2="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -agentlib:jprof=gencalib,logpath=/home/compatibility/executionNode"
	co3="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 "
fi

arg1="$arg -co \"$co1\""
arg2="$arg -co \"$co2\""
arg3="$arg -co \"$co3\""

if [ -z $1 ]; then
	script_test=1
else
	script_test=$1
fi
profile_duration=50

USE_LARGEPAGE=0
JIT=0;
app_start=1
app_end=1
duration_start=60
duration_end=60
size_tuple_start=8
size_tuple_end=8
task_type_start=0
task_type_end=0
I_C_start=10000
I_C_end=40000
O_C_start=1
O_C_end=1
window_start=2
window_end=2
size_state_start=8
size_state_end=8

function profile {
	case $1 in
	1)	#General Exploration with CPU concurrency and Memory Bandwidth
		#amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
		amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=$profile_duration $searchdir -result-dir $outputPath/resource >> $outputPath/profile1.txt;;
	2)	#general
		#amplxe-cl -duration=200 -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir --start-paused --resume-after 10 --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt;;
		#amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=$vtune_duration -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt &
		amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=$profile_duration  --target-pid $r -result-dir $outputPath/general >> $outputPath/profile2.txt &
	;;
	6)	#context switch
		amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $outputPath/context >> $outputPath/profile6.txt;;
	4)	#Remote memory
		#amplxe-cl -collect-with runsa -knob event config=OFFCORE_RESPONSE.ALL_DATA_RD.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_CODE_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HITM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HIT_FORWARD_0:sa=100003 -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/remote >> $MY_PATH2/profile4.txt;;
		 amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.DEMAND_CODE_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003 $searchdir --target-pid $r -result-dir $outputPath/general >> $outputPath/profile4.txt;;
	5)	#intel PMU
		#toplev.py -l3 --no-desc -x, sleep 100 -o $MY_PATH2/profile5.txt
		toplev.py -l3 sleep 10
		;;
	3) 	#ocperf
		profile_RMA.sh $outputPath/profile3.txt $profile_duration
		;;
	7)	#IMC
		perf stat -e -a -per-socket  uncore_imc_0/event=0x4,umask=0x3/,uncore_imc_1/event=0x4,umask=0x3/,uncore_imc_4/event=0x4,umask=0x3/,uncore_imc_5/event=0x4,umask=0x3/
		;;
	8)	#ocperf PID
		 ./profile_RMA_PID.sh $outputPath/profile8.txt $r
		;;
	9)	#ocperf PID LLC
		 ./profile_LLC_PID.sh $outputPath/profile9.txt $r
	esac
}

#app from 1:4
#SG(1),AC,TM,LR(4)
app=$app_start
while [ $app -le $app_end ] ;
do
    bt=1
    bt_end=1
    while [ $bt -le $bt_end ] ;
    do
        duration=$duration_start
        while [ $duration -le $duration_end ];
        do
                size_tuple=$size_tuple_start
                while [ $size_tuple -le $size_tuple_end ];
                do
                    task_type=$task_type_start
                    while [ $task_type -le $task_type_end ];
                    do
                        I_C=$I_C_start
                        while [ $I_C -le $I_C_end ];
                        do
                            O_C=$O_C_start
                            while [ $O_C -le $O_C_end ];
                            do
                                window=$window_start
                                while [ $window -le $window_end ];
                                do
                                    size_state=$size_state_start
                                    while [ $size_state -le $size_state_end ];
                                    do
                                        arg="-bt $bt -m remote --runtime $duration --size_tuple $size_tuple --task_type $task_type --I_C $I_C  --O_C $O_C
                                        --window $window --size_state $size_state -sink_class $SINK_CLASS -spout_class $SPOUT_CLASS"
                                        x=$x_start
                                        x_end=$x_start_end
                                        while [ $x -le $x_end ] ;
                                        do
                                            ct4=$down
                                            ct4_end=$up
                                            while [ $ct4 -le  $ct4_end ] ;
                                            do
                                                ct3=$down
                                                ct3_end=$up
                                                while [ $ct3 -le  $ct3_end ] ;
                                                do
                                                    ct2=$down
                                                    ct2_end=$up
                                                    while [ $ct2 -le  $ct2_end ] ;
                                                    do
                                                        ct1=$down
                                                        ct1_end=$up
                                                        while [ $ct1 -le  $ct1_end ] ;
                                                        do
                                                            count_number=1000
                                                            case $app in
                                                                1)
                                                                    #disable ct2,3 and 4.
                                                                    let ct2_end=1
                                                                    let ct2=$ct2_end+1
                                                                    let ct3_end=1
                                                                    let ct3=$ct3_end+1
                                                                    let ct4_end=1
                                                                    let ct4=$ct4_end+1

                                                                    echo "run microbenchmark with batch:$bt thread:$ct1 --runtime $duration --size_tuple $size_tuple --task_type $task_type --I_C $I_C  --O_C $O_C --window $window --size_state $size_state"
                                                                    outputPath=$Common_PATH/output_microbenchmark/$bt\_$ct1\_$duration\_$size_tuple\_$task_type\_$I_C\_$O_C\_$window\_$size_state
                                                                    if [  ! -s  $outputPath/throughput.txt ] ; then
                                                                    if [ $script_test == 0 ] ; then
                                                                        mkdir -p $outputPath

                                                                        if [ $USE_LARGEPAGE == 1 ] ; then
                                                                         storm jar $JAR_PATH $MAIN mytest $arg -co "$co1" -a "microbenchmark"  -mp $outputPath -ct1 $ct1
                                                                        else
                                                                            if [ $JIT == 1 ] ; then
                                                                                echo "run microbenchmark with JIT logging"
                                                                                storm jar $JAR_PATH $MAIN mytest $arg -co "$co2" -a "microbenchmark" -mp $outputPath  -ct1 $ct1
                                                                            else
                                                                                storm jar $JAR_PATH $MAIN mytest $arg -co "$co3" -a "microbenchmark" -mp $outputPath -ct1 $ct1
                                                                            fi
                                                                        fi
                                                                    fi
                                                                    fi
                                                                    ;;
                                                            esac
                                                            if [ $script_test == 0 ] ; then
                                                            if [  $x != 0 ] ; then
                                                                rm  $outputPath/sink_threadId.txt
                                                                while [ ! -s  $outputPath/sink_threadId.txt ]
                                                                do
                                                                    sleep 1
                                                                done
                                                                    r=$(<$outputPath/sink_threadId.txt)
                                                                jstack $r > $outputPath/threaddump_$x.txt
                                                            fi

                                                                while [ ! -s  $outputPath/throughput.txt ]
                                                                    do
                                                                        echo wait for application:$app
                                                                        sleep 5
                                                                    done
                                                                if [  $x != 0 ] ; then
                                                                    mkdir -p $HOME/vtune/$app/$x
                                                                    mv $outputPath $HOME/vtune/$app/$x
                                                                                fi
                                                                    profile $x
                                                            fi
                                                            sleep 5
                                                        let ct1=$ct1+4
                                                        done # end of thread 1
                                                    let ct2=$ct2+4
                                                    done # end of thread 2
                                                let ct3=$ct3+4
                                                done # end of thread 3
                                            let ct4=$ct4+4
                                            done # end of thread 4
                                        let x=$x+1
                                        done #x profiling loop
                                    let size_state=$size_state*$size_state
                                    done #state size loop
                                let window=$window*2
                                done
                            let O_C=$O_C*1000
                            done
                        let I_C=$I_C*2
                        done
                    let task_type=$task_type+1
                    done
                let size_tuple=$size_tuple*$size_tuple
                done
        let duration=$duration+300
        done
    let bt=$bt*2
    done #bt batch loop
let app=$app+1
done #app loop
