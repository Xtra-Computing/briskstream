#!/bin/bash
JAVA_HOME="/home/compatibility/Documents/jdk1.8.0_77"
USE_LARGEPAGE="0"
JIT="0"
#vm="384"
vm="128"
let up="$vm / 8"
num_workers="1"
bt="1"
bt_end="1"
xmx=Xmx$vm\g
xms=Xms$vm\g
#set -x
searchdir="--search-dir all:rp=/home/compatibility/compatibility-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=/home/compatibility/Documents/apache-compatibility-1.0.0/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=/home/compatibility/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"

arg="-n $num_workers -bt $bt -m remote"
co1="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages"
co2="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -javaagent:/home/compatibility/Documents/intrace-agent.jar"
co3="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"

arg1="$arg -co \"$co1\""
arg2="$arg -co \"$co2\""
arg3="$arg -co \"$co3\""

while [ $bt -le $bt_end ] ;
do
	x=0
	x_end=0
	while [ $x -le $x_end ] ;
	do
		#app from 4:10 
		#WC(4),FD,LG,SD,VS,TM,LR(10)
		app=4
		app_end=4
		while [ $app -le $app_end ] ;
		do
		
			ct11=1			
			ct11_end=$up
			while [ $ct11 -le  $ct11_end ] ;
			do
				ct10=1
				ct10_end=$up
				while [ $ct10 -le  $ct10_end ] ;
				do
					ct9=1
					ct9_end=$up
					while [ $ct9 -le  $ct9_end ] ;
					do
						ct8=1
						ct8_end=$up
						while [ $ct8 -le  $ct8_end ] ;
						do
							ct7=1
							ct7_end=$up
							while [ $ct7 -le  $ct7_end ] ;
							do
								ct6=1
								ct6_end=$up
								while [ $ct6 -le  $ct6_end ] ;
								do
									ct5=1
									ct5_end=$up
									while [ $ct5 -le  $ct5_end ] ;
									do
										ct4=1
										ct4_end=$up
										while [ $ct4 -le  $ct4_end ] ;
										do
											ct3=1
											ct3_end=$up
											while [ $ct3 -le  $ct3_end ] ;
											do
												ct2=1
												ct2_end=$up
												while [ $ct2 -le  $ct2_end ] ;
												do 
													ct1=1											
													ct1_end=$up
													while [ $ct1 -le  $ct1_end ] ;
													do
				echo $ct1 $ct2 $ct3 $ct4 $ct5 $ct6 $ct7 $ct8 $ct9 $ct10 $ct11
				case $app in  
					4) 
						count_number=4
						let ct4=$ct4_end+1
						let ct5=$ct5_end+1
						let ct6=$ct6_end+1
						let ct7=$ct7_end+1
						let ct8=$ct8_end+1
						let ct9=$ct9_end+1
						let ct10=$ct10_end+1
						let ct11=$ct11_end+1
						MY_PATH2=/media/tony/ProfilingData/output_word-count/$bt\_$ct1\_$ct2\_$ct3
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co1" -a word-count -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
						else
							if [ $JIT == 1 ] ; then
								echo "run word-count with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co2" -a word-count -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co3" -a word-count -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
							fi
						fi					
						;;
					5) 
						count_number=100
						let ct3=$ct3_end+1
						let ct4=$ct4_end+1
						let ct5=$ct5_end+1
						let ct6=$ct6_end+1
						let ct7=$ct7_end+1
						let ct8=$ct8_end+1
						let ct9=$ct9_end+1
						let ct10=$ct10_end+1
						let ct11=$ct11_end+1						
						MY_PATH2=/media/tony/ProfilingData/output_fraud-detection/$bt\_$ct1\_$ct2
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co1" -mp $MY_PATH2  -a fraud-detection -cn $count_number-ct1 $ct1 -ct2 $ct2
						else
							if [ $JIT == 1 ] ; then
								echo "run fraud-detection with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co2" -mp $MY_PATH2 -a fraud-detection -cn $count_number -ct1 $ct1 -ct2 $ct2
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co3" -mp $MY_PATH2 -a fraud-detection -ct1 $ct1 -ct2 $ct2

							fi
						fi					
						;;
					6) 
						count_number=2
						let ct6=$ct6_end+1
						let ct7=$ct7_end+1
						let ct8=$ct8_end+1
						let ct9=$ct9_end+1
						let ct10=$ct10_end+1
						let ct11=$ct11_end+1
						MY_PATH2=/media/tony/ProfilingData/output_log-processing/$bt\_$ct1\_$ct2\_$ct3\_$ct4\_$ct5
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co1" -mp $MY_PATH2 -a executionNode-processing -cn $count_number -n $num_workers -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5
						else
							if [ $JIT == 1 ] ; then
								echo "run executionNode-processing with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co2" -mp $MY_PATH2 -a executionNode-processing -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co3" -mp $MY_PATH2 -a executionNode-processing -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5
							fi
						fi					
						;;
					7) 
						count_number=4
						let ct4=$ct4_end+1
						let ct5=$ct5_end+1
						let ct6=$ct6_end+1
						let ct7=$ct7_end+1
						let ct8=$ct8_end+1
						let ct9=$ct9_end+1
						let ct10=$ct10_end+1
						let ct11=$ct11_end+1
						MY_PATH2=/media/tony/ProfilingData/output_spike-detection/$bt\_$ct1\_$ct2\_$ct3
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co1" -mp $MY_PATH2 -a spike-detection -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
						else
							if [ $JIT == 1 ] ; then
								echo "run executionNode-processing with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co2" -mp $MY_PATH2 -a spike-detection -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest $arg -co "$co3" -mp $MY_PATH2 -a spike-detection -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
							fi
						fi					
						;;
					8) 
						count_number=1
						MY_PATH2=/media/tony/ProfilingData/output_voipstream/$bt\_$ct1\_$ct2\_$ct3\_$ct4\_$ct5\_$ct6\_$ct7\_$ct8\_$ct9\_$ct10\_$ct11
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a voipstream $arg -co "$co1" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10 -ct11 $ct11
						else
							if [ $JIT == 1 ] ; then
								echo "run executionNode-processing with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a voipstream $arg -co "$co2" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10 -ct11 $ct11
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a voipstream $arg -co "$co3" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10 -ct11 $ct11
							fi
						fi					
						;;
					9) 
						count_number=1
						let ct3=$ct3_end+1
						let ct4=$ct4_end+1
						let ct5=$ct5_end+1
						let ct6=$ct6_end+1
						let ct7=$ct7_end+1
						let ct8=$ct8_end+1
						let ct9=$ct9_end+1
						let ct10=$ct10_end+1
						let ct11=$ct11_end+1
						MY_PATH2=/media/tony/ProfilingData/output_traffic-monitoring/$bt\_$ct1\_$ct2
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a traffic-monitoring $arg -co "$co1" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2
						else
							if [ $JIT == 1 ] ; then
								echo "run executionNode-processing with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a traffic-monitoring $arg -co "$co2" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a traffic-monitoring $arg -co "$co3" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2
							fi
						fi					
						;;
					10) 
						count_number=1
						let ct11=$ct11_end+1
						MY_PATH2=/media/tony/ProfilingData/output_linear-road-full/$bt\_$ct1\_$ct2\_$ct3\_$ct4\_$ct5\_$ct6\_$ct7\_$ct8\_$ct9\_$ct10
						mkdir -p $MY_PATH2
						if [ $USE_LARGEPAGE == 1 ] ; then
						 storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a linear-road-full $arg -co "$co1" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10
						else
							if [ $JIT == 1 ] ; then
								echo "run executionNode-processing with JIT logging"
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a linear-road-full $arg -co "$co2" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10
							else
								storm jar /home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar flink.applications.BriskRunner mytest -a linear-road-full $arg -co "$co3" -mp $MY_PATH2 -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -ct4 $ct4 -ct5 $ct5 -ct6 $ct6 -ct7 $ct7 -ct8 $ct8 -ct9 $ct9 -ct10 $ct10
							fi
						fi					
						;;
			esac	 		
			if [  $x != 0 ] ; then
				rm  $MY_PATH2/spout_threadId.txt
				while [ ! -s  $MY_PATH2/spout_threadId.txt ]
				do
					sleep 20
				done
				r=$(<$MY_PATH2/spout_threadId.txt)
				echo "$r"
				jstack $r >> $MY_PATH2/threaddump_$x.txt
			fi
			case $x in
			1)	#General Exploration with CPU concurrency and Memory Bandwidth
				amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
			2)	#general
				amplxe-cl -duration=200 -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003 -data-limit=1024 $searchdir --start-paused --resume-after 10 --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt;;
			3)	#context switch
				amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/context >> $MY_PATH2/profile3.txt;;
			4)	#Remote memory
				amplxe-cl -collect-with runsa -knob event config=OFFCORE_RESPONSE.ALL_DATA_RD.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_CODE_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HITM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HIT_FORWARD_0:sa=100003 -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/remote >> $MY_PATH2/profile4.txt;;
			esac
			while [ ! -s  $MY_PATH2/sink.txt ]
				do
					echo wait for application:$app
					sleep 20
				done
			
													let ct1=$ct1*2
													done # end of thread 1
												let ct2=$ct2*2
												done # end of thread 2
											let ct3=$ct3*2
											done # end of thread 3
										let ct4=$ct4*2
										done # end of thread 4
									let ct5=$ct5*2
									done # end of thread 5
								let ct6=$ct6*2
								done # end of thread 6
							let ct7=$ct7*2
							done # end of thread 7
						let ct8=$ct8*2
						done # end of thread 8
					let ct9=$ct9*2
					done # end of thread 9
				let ct10=$ct10*2
				done # end of thread 10
			let ct11=$ct11*2
			done # end of thread 11
		let app=$app+1
		done #app loop
	let x=$x+1	
	done #x profiling loop
let bt=$bt*2
done #bt batch loop
