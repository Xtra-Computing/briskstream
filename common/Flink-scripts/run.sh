#!/bin/bash
searchdir="--search-dir all:rp=$HOME/Documents/briskstream --search-dir all:rp=$JAVA_HOME/lib  --search-dir all:rp=/opt/intel/vtune_amplifier_xe_2017/bin64"
MAIN="applications.FlinkRunner"
function profile {

    while [ ! -s  $2/sink_threadId.txt ]
        do
            echo "wait for sink id"
            sleep 1
    done
    r=$(<$2/sink_threadId.txt)

	echo "$r"
	jstack $r >> $2/threaddump_$x.txt
	case $1 in
	1)	#General Exploration with CPU concurrency and Memory Bandwidth
		#amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
		amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100  --target-pid $r -result-dir $2/general >> $2/profile1.txt ;;
	2)	#general
		amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=20  --target-pid $r -result-dir $2/custom >> $2/profile2.txt 
	;;
	6)	#context switch
		amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $outputPath/context >> $outputPath/profile6.txt;;
	4)	#Remote memory
		 amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.DEMAND_CODE_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003 $searchdir --target-pid $r -result-dir $outputPath/rma >> $outputPath/profile4.txt;;
	5)	#intel PMU
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

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
        bash ~/start-flink.sh
		sleep 15
		argument="application：$3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6"
		# echo $argument
		arg_benchmark="--THz $2 --runtime 50  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple $size_tuple "
		arg_application="-algo.st 5 -sit 10 -tt $6 -input $input -bt $bt --native -m remote  -" # --tune  --random  --worst
		
		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native/$2/$4\_$6\_$size_tuple 
		mkdir -p $path
		echo "native phase:" $argument >> $path/test\_$input\_$bt.txt
		if [ $Profile == 1 ] ; then
			flink run $JAR_PATH $MAIN mytest -co "$JVM_args1" $arg_benchmark -mp $outputPath/native/$2 $arg_application --relax 1 -a $3 --machine $machine >> $path/test\_$input\_$bt.txt		&
			profile $profile_type $path
		else
			flink run $JAR_PATH $MAIN mytest -co "$JVM_args1" $arg_benchmark -mp $outputPath/native/$2 $arg_application --relax 1 -a $3 --machine $machine >> $path/test\_$input\_$bt.txt
		fi
      
        # cat $path/test\_$input\_$bt.txt | grep "finished measurement (k events/s)"
        # cat $path/test\_$input\_$bt.txt | grep "finished measurement (k events/s)" >> $outputPath/native_throughput\_$2\_$4\_$5\_$input\_$bt.txt	

        while [ ! -s  $path/throughput.txt ]
        do
            echo wait for application:$app
            sleep 5
        done
        cat $path/throughput.txt
        sleep 10
        
}
	
# Configurable variables
output=test.csv
# Generate a timestamp
timestamp=$(date +%Y%m%d-%H%M)
# Create a temporary directory
app_cnt=0
cnt=0
for app in  "WordCount" #"FraudDetection" "SpikeDetection" "LogProcessing"  "LinearRoad" #"StreamingAnalysis" "WordCount" "FraudDetection" "SpikeDetection" "LogProcessing"  "LinearRoad"
do
    machine=0
    for percentile in 0 #90 # 100 #99 100 ##the percentile used in profiling..
    do    
	outputPath=$HOME/Documents/briskstream/Flink-Tests/test-$timestamp/$app/$percentile
	mkdir -p $outputPath
	cd $outputPath
	# Save some system information
	uname -a > kernel.txt
	cat /proc/cpuinfo > cpuinfo.txt
	cat /proc/meminfo > meminfo.txt

	echo Benchmark initiated at $(date +%Y%m%d-%H%M)
    
    GC_args="-Xloggc:$outputPath-gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCCause -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=5M"
	JVM_args1="-Xms1024g -Xmx2048g -XX:+UseG1GC"
	JAR_PATH="$HOME/Documents/briskstream/FlinkBenchmarks/target/Flink-1.3.2-jar-with-dependencies.jar"
	Profile=0 #vtune profile
	profile_type=1
    ##The below is for the case that #theads=15
	#SA=(5000	10000	15000	25000	 )
	# WC=(5000	10000	15000	25000	53416  )
	# FD=(5000	10000	15000   25000	86017  ) 
	# SD=(5000	10000	15000   25000	66974 )
	# TM=(5000	10000	15000   25000	38 )
	# LG=(5000	10000	15000   25000	41182 )
	# VS=(5000	10000	15000   25000	28817 )
	# LR=(5000	10000	15000   25000	46506 )
	
	#10 different input HZ.
	# SA=(150000) # 900 1800 2700 3600 4500 9000 18000 27000 36000 45000
	# WC=(53416 )
	# FD=(86017 ) 
	# SD=(66974 )
	# TM=(38 )
	# LG=(41182 )
	# VS=(28817 )
	# LR=(46506 )

    WC=(5000000 )
	FD=(450000 ) 
	SD=(900000 )
	# TM=(450000 )
	LG=(600000 )
	VS=(450000 )
	LR=(320000 )
    
	#5 repeats
	for input in 1
	do
		case "$app" in
			"StreamingAnalysis")
				for hz in "${SA[@]}"
				do
						#8 configurations of profile plan
					for profile_plan in 0 #1 #2 3 4 5 6 7
					do
							#10 configurations of tuple size
						for size_tuple in 1 16 64 128 256 512 # 
						do
							for window in 2 #4 8
							do
								#complexity of sink
								for complexity in 0 10 100 1000
								do
									# for repeate in 1 2 3 4 5
									# do
										statistics_profile $Profile $hz $app $size_tuple $profile_plan $complexity $window
									# done
								done
							done
						done
					done				
				done
				;;
			"WordCount")
				for hz in "${WC[@]}"
				do
					echo "WordCount Study"
					####   ####$size_tuple $profile_plan $complexity $window
                    # for bt in 100
                    # do
                        # statistics_profile $hz $app 0 -1 0 0 $bt
                    # done
                    
					# for num_socket in 2 4 8
					# do
						# for cores in 8
						# do
							# for pt in 1 #4 16
							# do
								# for ct1 in 1 #4 16
								# do
									# for ct2 in  1 #4 16
									# do
										# for tt in   1 
										# do
											# main_aware $Profile $hz $pt $ct1 $ct2 $app $num_socket $cores $tt $input
										# done
									# done
								# done
							# done
						# done
					# done    
                    
                    # -eq # equal
                    # -ne # not equal
                    # -lt # less than
                    # -le # less than or equal
                    # -gt # greater than
                    # -ge # greater than or equal
                    for bt in 100
                    do
                        # statistics_profile $hz $app 0 -1 0 0 $bt $percentile
                        for size_tuple in 8 64  512 4096 #8 64 ##  128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                            for tt in  125 150 175 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                            do
                                 main_native $Profile $hz $app 8 -1 $tt $input $bt $size_tuple
    #                            main_aware_worst $Profile $hz $app 8 -1 $tt $input $bt
    #                            i="0"
    #                            while [ $i -lt 1 ]
    #                            do
    #                                main_random $Profile $hz $app 8 -1 $tt $input $bt $i
    #                                i=$[$i+1]
    #                            done
    #                            main_aware $Profile $hz $app 8 -1 $tt $input $bt
                            done                     
                            # for tt in 135 115 95 75 55 35 #15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                            # do
                                # main_native $Profile $hz $app 8 -1 $tt $input $bt
                                # main_aware $Profile $hz $app 8 -1 $tt $input $bt                           
                            # done   
                        done
                    done   
				done
				;;
			"FraudDetection")
				for hz in "${FD[@]}"
				do
					echo "FraudDetection Study"
					for bt in 100 #10 100
                    do
                        for tt in  150 175 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                             main_native $Profile $hz $app 8 -1 $tt $input $bt
                        done               
                    done   
				done
				;;
			"SpikeDetection")
				for hz in "${SD[@]}"
				do
					echo "SpikeDetection Study"
					for bt in 100 #10 100
                    do
                        for tt in  150 170 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                             main_native $Profile $hz $app 8 -1 $tt $input $bt
                        done               
                    done                          
				done
				;;   
			# "TrafficMonitoring")
				# for hz in "${TM[@]}"
				# do
					# echo "TrafficMonitoring Study"
					# statistics_profile $hz $app 0 -1 0 0
					# for num_socket in 2 4 8
					# do
						# for cores in 8
						# do	
							# for pt in 1 #4 16
							# do                          
                                # for ct1 in 1 #16 8 4
                                # do
                                    # for ct2 in 1 #16 8 4
                                    # do
                                       
                                            # for tt in   1 
                                            # do
                                                # main_aware $Profile $hz $pt $ct1 $ct2 $app $num_socket $cores $tt $input
                                            # done 
                                       
                                    # done
                                # done
                            # done
						# done
					# done
                    # for tt in  5 25 45 65 85
                    # do
                        # main_aware $Profile $hz 1 1 1 $app 8 8 $tt $input
                        # main_native $Profile $hz 1 1 1 $app 8 8 $tt $input   
                    # done                        
				# done
				# ;; 		
			"LogProcessing")
				for hz in "${LG[@]}"
				do
					echo "Log Processing Study"
					for bt in 100 #10 100
                    do
                        for tt in 175 195 205 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                             main_native $Profile $hz $app 8 -1 $tt $input $bt
                        done               
                    done                       
				done
				;; 		
			"VoIPSTREAM")
				for hz in "${VS[@]}"
				do
					echo "VoIPSTREAM Study"
					for bt in 100 #10 100
                    do
                        for tt in  150 175 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                             main_native $Profile $hz $app 8 -1 $tt $input $bt
                        done               
                    done     
				done
				;; 		
			"LinearRoad")
				for hz in "${LR[@]}"
				do  
                    echo "LinearRoad Study"
					for bt in 100 #10 100
                    do
                        for tt in  150 175 #128 198 288 15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                        do
                             main_native $Profile $hz $app 8 -1 $tt $input $bt
                        done               
                    done      
				done
				;; 					
			*)
				echo $"Usage: $0 {application}"
				exit 1
		esac 
	done #input rate percentage
    done #varying percentage
done #varing apps.
