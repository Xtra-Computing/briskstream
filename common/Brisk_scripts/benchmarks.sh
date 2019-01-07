#!/bin/bash
function profile {

    cnt=0
    while [ ! -s  $2/sink_threadId.txt ]
        do
            echo "wait for sink id $cnt"
            let cnt=cnt+1
            sleep 1
    done
    r=$(<$2/sink_threadId.txt)

	echo "$r"
	jstack $r >> $2/threaddump.txt
	case $1 in
    4)	#HPC-performance
		 amplxe-cl -collect hpc-performance -data-limit=1024 --target-pid $r -result-dir $2/hpc >> $2/profile4.txt;;
	esac
}

#main_toff $Profile $hz $app 8 -1 $tt $input $bt
function local_execution {
        #require: $argument $path $input $bt $Profile $arg_application $app $machine $num_socket $num_cpu $hz
        # echo "streaming phase:" $argument >> $path/test\_$input\_$bt.txt -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
#killall -9 java
#clean_cache
        JVM_args_local="-Xms2g -Xmx50g -XX:ParallelGCThreads=$tt -XX:CICompilerCount=2"

		if [ $Profile == 1 ] ; then
			 java $JVM_args_local -jar $JAR_PATH $arg_benchmark $arg_application >> $path/$tt\_$TP.txt		&
			 profile $profile_type $path
		else
			 java $JVM_args_local -jar $JAR_PATH $arg_benchmark $arg_application>> $path/$tt\_$TP.txt
		fi

        cat $path/$tt\_$TP.txt | grep "finished measurement (k events/s)"
}

function read_only_test {
        path=$outputPath/$hz/$CCOption/$checkpoint
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function read_only_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read --measure" #

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function write_intensive_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function write_intensive_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read --measure" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function working_set_size_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$NUM_ACCESS
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function working_set_size_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint/$NUM_ACCESS
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read --measure" #

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function Read_Write_Mixture_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$ratio_of_read
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function DB_SIZE_TEST {
        path=$outputPath/$hz/$CCOption/$checkpoint/$ratio_of_read
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}



function Read_Write_Mixture_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint/$ratio_of_read
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read --measure" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function checkpoint_interval_test {
        path=$outputPath/$hz/$CCOption/$checkpoint
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function partition_test {
        path=$outputPath/$hz/$CCOption/$checkpoint
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function multi_partition_test {
        path=$outputPath/$hz/$CCOption/$ratio_of_read/$number_partitions/$ratio_of_multi_partition
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --NUM_ITEMS $NUM_ITEMS --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function CrossTables_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}


function OnlineBiding_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function CrossTables_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition --measure" #

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function PositionKeeping_test {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $thetan --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition" #--measure

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function PositionKeeping_breakdown {
        path=$outputPath/$hz/$CCOption/$checkpoint/$theta
		arg_benchmark="--machine $machine --runtime 30 --loop 1000 -st $st -input $iteration -sit 1 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction -bt $bt --native --relax 1 -a $app -mp $path"
		arg_application="--THz $hz -tt $tt --CCOption $CCOption --TP $TP --checkpoint $checkpoint --theta $theta --ratio_of_read $ratio_of_read --number_partitions $number_partitions --ratio_of_multi_partition $ratio_of_multi_partition --measure" #

		#####native execution
		echo "==benchmark:$benchmark settings:$arg_application path:$path=="
		mkdir -p $path
        local_execution $path $hz $tt $CCOption $TP $checkpoint $theta $NUM_ACCESS $ratio_of_read $theta
}

function clean_cache {
    echo 3 | sudo tee /proc/sys/vm/drop_caches
}

# Configurable variables
output=test.csv
# Generate a timestamp
timestamp=$(date +%Y%m%d-%H%M)
FULL_SPEED_TEST=("PositionKeeping" "CrossTables" "Read_Only" "Write_Intensive" "Read_Write_Mixture" "Interval" "Partition" "MultiPartition") # "Working_Set_Size"
FULL_BREAKDOWN_TEST=("PositionKeepingBreakdown" "CrossTablesBreakdown" "Read_Only_Breakdown" "Write_Intensive_Breakdown" "Read_Write_Mixture_Breakdown")
for benchmark in "CrossTables" #"CrossTables" #"Partition" "MultiPartition" #"Interval" "CrossTablesBreakdown" "Read_Only_Breakdown" "Write_Intensive_Breakdown" "Working_Set_Size_Breakdown" "Read_Write_Mixture_Breakdown"
do
    app="MicroBenchmark"
    machine=3 #RTM.
    Profile=0 #vtune profile: 0 disable, 1 enable.
	profile_type=4 # 1 for general..4 for hpc.
	JAR_PATH="$HOME/briskstream/BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar"

    outputPath=$HOME/briskstream/Tests/test-$timestamp/$benchmark
	mkdir -p $outputPath
	cd $outputPath
	# Save some system information
	uname -a > kernel.txt
	cat /proc/cpuinfo > cpuinfo.txt
	cat /proc/meminfo > meminfo.txt

	echo $benchmark Benchmark initiated at $(date +%Y%m%d-%H%M)
    HZ=(500000) #max Hz.
	let "st = 1"
    let "gc_factor = 0"
    let "socket = 4"
    let "cpu = 10"
    let "bt = 1"
    let "gc_factor = 0"
    let "iteration = 1"
    checkpoint=0.25
    NUM_ITEMS=100000 #smaller means higher contention!
        case "$benchmark" in
            "Read_Only")
                #4 * 6 * 1 * 1 * (2 mins) = ~ 48 mins
                for hz in "${HZ[@]}"
                do
                    for theta in 0
                    do
                        for tt in 32 24 16 8 2 # * 6
                        do
                            for CCOption in 3 # 0 1 2 3 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 1
                                    do
                                        let "TP = $tt"
                                        read_only_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Write_Intensive")
                # 4 * 6 * 2 * 1 * (2 mins) = 96 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6 0.8 #0.6
                    do
                        for tt in 32 24 16 8 2 # * 6
                        do
                            for CCOption in 3 # 0 1 2 3 #2 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 0
                                    do
                                        let "TP = $tt"
                                        write_intensive_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Working_Set_Size") # 4 * 6 * 6 * 1 * 1 * (2 mins) = 288 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38
                        do
                            for CCOption in 0 1 2 3
                            do
                                for NUM_ACCESS in 100 10 1  #10 8 6 4 2 1 # *6
                                do
                                    for ratio_of_read in 0
                                    do
                                        let "TP = $tt"
                                        working_set_size_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Read_Write_Mixture") # 4 * 5 * 6 * 1 * 1 * (2 mins) = 240 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38
                        do
                            for CCOption in 3 #0 1 2 3
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 0.5 #0 0.25 0.5 0.75 1
                                    do
                                        let "TP = $tt"
                                        Read_Write_Mixture_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Interval") # 1 * 6 * 6 * 1 * 1 * (2 mins) = 72 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.0
                    do
                        for tt in 38 #32 24 16 8 2
                        do
                            CCOption=3
                            for checkpoint in 0.025 0.05 0.1 0.5 0.75 1 #skipped 0.25
                            do
                               for x in 1 #25 50 75 100
                               do

                                  for NUM_ACCESS in 10 #8 6 4 2 1
                                  do
                                       let "TP = $tt/$x"
#                                      checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                  done
                               done
                            done

#                            for CCOption in 0 1 2 #default ? seconds.
#                            do
#                                for NUM_ACCESS in 10 #8 6 4 2 1
#                                do
#                                    ratio_of_read=1
#                                   checkpoint=0.1
#                                    TP=$tt
#                                    checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
#                                done
#                            done
                        done # Threads/Cores
                    done #Theta
    
                    for theta in 0.6
                    do
                        for tt in 38 #32 24 16 8 2
                        do
                            CCOption=3
                            for checkpoint in 0.025 0.05 0.1 0.5 0.75 1 #skipped 0.25
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 10 #8 6 4 2 1
                                  do
                                      ratio_of_read=0
#                                     checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                  done
                               done
                            done

#                            for CCOption in 0 1 2 #default ? seconds.
#                            do
#                                for NUM_ACCESS in 10 #8 6 4 2 1
#                                do
#                                    ratio_of_read=0
#                                   checkpoint=0.1
#                                    TP=$tt
#                                    checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
#                                done
#                            done

                        done # Threads/Cores
                    done #Theta
    
                    for theta in 0.6
                    do
                        for tt in 38 #32 24 16 8 2
                        do
                            CCOption=3
                            for checkpoint in 0.025 0.05 0.1 0.5 0.75 1 #skipped 0.25
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 10 #8 6 4 2 1
                                  do
                                      ratio_of_read=0.5
                                      checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                  done
                               done
                            done

                            for CCOption in 0 1 2 4 #default ? seconds.
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    ratio_of_read=0.5
                                    checkpoint=0.1
                                    TP=$tt
#                                    checkpoint_interval_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                done
                            done

                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Partition")  # 1 * 6 * 1 * 1 * 1 * (2 mins) = 12 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0 #0.6 0.8
                    do
                        for tt in 38 #32 24 16 8 2 #38 32
                        do
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 1
                                    do
                                        TP=$tt
                                        number_partitions=1
                                        ratio_of_multi_partition=0
                                        partition_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $number_partitions $ratio_of_multi_partition
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                    for theta in 0.6
                    do
                        for tt in 38 #32 24 16 8 2 #38 32
                        do
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=$tt
                                        number_partitions=1
                                        ratio_of_multi_partition=0
                                        partition_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $number_partitions $ratio_of_multi_partition
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "MultiPartition")  # 1 * 6 * 1 * 1 * 1 * (2 mins) = 12 mins.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6 #0.6 0.8
                    do
                        for tt in 38
                        do
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 0 1
                                    do
                                        for number_partitions in 2 4 6 10
                                        do
                                            TP=$tt
                                            ratio_of_multi_partition=0.5
                                            multi_partition_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $number_partitions $ratio_of_multi_partition
                                        done
                                    done
                                    for ratio_of_read in 0 1
                                    do
                                        for ratio_of_multi_partition in 0.25 0.5 0.75 1
                                        do
                                            TP=$tt
                                            number_partitions=6
                                            multi_partition_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $number_partitions $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "DB_SIZE") # 4 * 5 * 6 * 1 * 1 * (2 mins) = 240 mins.
                for NUM_ITEMS in 10000 #100000
                do
                    for hz in "${HZ[@]}"
                    do
                        for theta in 0.6
                        do
                            for tt in 38
                            do
                                for CCOption in 0 1 2 3 4
                                do
                                    for NUM_ACCESS in 10 #8 6 4 2 1
                                    do
                                        for ratio_of_read in 0 0.25 0.5 0.75 1
                                        do
                                            let "TP = $tt"
                                            Read_Write_Mixture_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                        done
                                    done
                                done
                            done # Threads/Cores
                        done #Theta
                    done #Input Hz
                done #varying DB size.
                ;;
            "CrossTables") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
                app="CrossTables"
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6 #0.8
                    do
                        for tt in 38 #32 24 16 8 2 #38 32 24 16 8 2
                        do
                            for CCOption in 3
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 1
                                    do
                                        TP=$tt
                                        for checkpoint in 0.025 0.05 0.1 0.25 0.5 0.75 1
                                        do
                                            ratio_of_multi_partition=0
                                            number_partitions=4
                                            CrossTables_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                             for CCOption in 1 2 4
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 1
                                    do
                                        TP=$tt
                                        for checkpoint in 1
                                        do
                                            ratio_of_multi_partition=0.5
                                            number_partitions=6
                                            #CrossTables_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done

                            rm
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "OnlineBiding") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
                app="OnlineBiding"
                NUM_ITEMS=10000
                ratio_of_multi_partition=0.75
                number_partitions=10
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6 #biding is contented..?
                    do
                        for tt in 2 8 16 24 32 38 #
                        do
                            rm /home/xtra/briskstream/EVENT -r #save space..
                            for CCOption in 3
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 1
                                    do
                                        for checkpoint in 0.025 0.05 0.1 0.25 0.5 0.75 1
                                        do
                                            TP=$tt
#                                            OnlineBiding_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                            for CCOption in 1 2 #4
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 1
                                    do
                                        TP=$tt
                                        for checkpoint in 1
                                        do
                                            OnlineBiding_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "PositionKeeping") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
                app="PositionKeeping"
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38 32 24 16 8 2
                        do
    
                            for CCOption in 1 2
                            do
                                for NUM_ACCESS in 1 #8 6 4 2 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=1
                                       checkpoint=0.1
                                        ratio_of_multi_partition=0
                                        number_partitions=1
    #                                    PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                    done
                                done
                            done
    
                            CCOption=3
                            for checkpoint in 1 #2 4 8 10 #default 1 seconds.
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 1 #fixed to be 1.
                                  do
                                      for ratio_of_read in 0
                                      do
                                          number_partitions=1
                                          ratio_of_multi_partition=0
                                          PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                      done
                                  done
                               done
                            done
    
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        for ratio_of_multi_partition in 0
                                        do
                                            TP=1
                                           checkpoint=0.1
                                            number_partitions=1
    #                                        PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
    
                    for theta in 0.2 0.4 0.6 0.8 1
                    do
                        for tt in 38
                        do
                            for CCOption in 1 2
                            do
                                for NUM_ACCESS in 1 #8 6 4 2 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=1
                                       checkpoint=0.1
                                        ratio_of_multi_partition=0
                                        number_partitions=1
    #                                    PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                    done
                                done
                            done
    
                            CCOption=3
                            for checkpoint in 1 #2 4 8 10 #default 1 seconds.
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 1 #fixed to be 1.
                                  do
                                      for ratio_of_read in 0
                                      do
                                          number_partitions=1
                                          ratio_of_multi_partition=0
    #                                      PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                      done
                                  done
                               done
                            done
    
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        for ratio_of_multi_partition in 0
                                        do
                                            TP=1
                                           checkpoint=0.1
                                            number_partitions=1
    #                                        PositionKeeping_test $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Read_Only_Breakdown") #5 * 6 * 1 * 3 * (2 mins) = ~ 180 mins ~ 3 hours.
                for hz in "${HZ[@]}"
                do
                    for theta in 0
                    do
                        for tt in 38 #32 24 16 8 2 # * 6
                        do
                            for CCOption in 3 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 1
                                    do
                                        TP=$tt
                                        read_only_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Write_Intensive_Breakdown") # 5 * 6 * 2 * 3 * (2 mins) = 360 mins ~ 6 hours.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6 0.8 # *2
                    do
                        for tt in 38 #32 24 16 8 2 # * 6
                        do
                            for CCOption in 0 #1 2 3 #0 1 2 # * (3 + 2)
                            do
                                for NUM_ACCESS in 10
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=$tt
                                        write_intensive_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Working_Set_Size_Breakdown") # 6 * 5 * 6 * 1 * 2 * (2 mins) = 720 mins ~ 12 hours.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38 #32 24 16 8 2
                        do
                            for CCOption in 0 1 2
                            do
                                for NUM_ACCESS in 1 # 16
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=1
                                       checkpoint=0.1
                                        working_set_size_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                            CCOption=3
                            for checkpoint in 1 #default 1 seconds.
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 1 # 16
                                  do
                                      for ratio_of_read in 0
                                      do
                                          working_set_size_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                      done
                                  done
                               done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "Read_Write_Mixture_Breakdown") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38 #32 24 16 8 2
                        do
                            for CCOption in 2 #0 1
                            do
                                for NUM_ACCESS in 10 #8 6 4 2 1
                                do
                                    for ratio_of_read in 0.5
                                    do
                                        TP=1
#                                        Read_Write_Mixture_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                    done
                                done
                            done
                            CCOption=3
                            for x in 1 #25 50 75 100
                            do
                               let "TP = $tt/$x"
                               for NUM_ACCESS in 10 #8 6 4 2 1
                               do
                                   for ratio_of_read in 0 0.25 0.5 0.75 1
                                   do
                                       Read_Write_Mixture_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read
                                   done
                               done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            "CrossTablesBreakdown") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
            app="CrossTables"
            for hz in "${HZ[@]}"
            do
                for theta in 0.6 #0.8
                do
                    for tt in 38 32
                    do
                        for CCOption in 3
                        do
                            for NUM_ACCESS in 1 #8 6 4 2 1
                            do
                                for ratio_of_read in 0.5
                                do
                                    TP=$tt
                                    ratio_of_multi_partition=0
                                    number_partitions=4
                                    CrossTables_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                done
                            done
                        done
                    done # Threads/Cores
                done #Theta
            done #Input Hz
            ;;
            "PositionKeepingBreakdown") # 5 * 5 * 6 * 1 * 3 * (2 mins) = 900 mins ~ 15 hours.
                app="PositionKeeping"
                for hz in "${HZ[@]}"
                do
                    for theta in 0.6
                    do
                        for tt in 38
                        do
                            for CCOption in 0 1 2
                            do
                                for NUM_ACCESS in 1 #8 6 4 2 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        TP=1
                                       checkpoint=0.1
                                        ratio_of_multi_partition=0
                                        number_partitions=1
                                        PositionKeeping_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                    done
                                done
                            done
    
                            CCOption=3
                            for checkpoint in 1 #2 4 8 10 #default 1 seconds.
                            do
                               for x in 1 #25 50 75 100
                               do
                                  let "TP = $tt/$x"
                                  for NUM_ACCESS in 1 #fixed to be 1.
                                  do
                                      for ratio_of_read in 0
                                      do
                                          number_partitions=1
                                          ratio_of_multi_partition=0
                                          PositionKeeping_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                      done
                                  done
                               done
                            done
    
                            for CCOption in 4 # * (3 + 2)
                            do
                                for NUM_ACCESS in 1
                                do
                                    for ratio_of_read in 0
                                    do
                                        for ratio_of_multi_partition in 0
                                        do
                                            TP=1
                                           checkpoint=0.1
                                            number_partitions=1
                                            PositionKeeping_breakdown $Profile $hz $app $socket $cpu $tt $iteration $bt $gc_factor $TP $CCOption $checkpoint $st $theta $NUM_ACCESS $ratio_of_read $ratio_of_multi_partition
                                        done
                                    done
                                done
                            done
                        done # Threads/Cores
                    done #Theta
                done #Input Hz
                ;;
            *)
                echo $"Usage: $0 {benchmark}"
                exit 1
        esac
done #varing benchmarks.
cd $HOME/scripts
./jobdone.py
