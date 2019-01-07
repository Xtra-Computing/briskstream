#!/bin/bash

for repeate in 1 #2 3 4 5
do
	# Configurable variables
	output=test.csv
	# Generate a timestamp
	timestamp=$(date +%Y%m%d-%H%M)
	# Create a temporary directory
	mkdir $HOME/Documents/briskstream/test-$timestamp
	cd $HOME/Documents/briskstream/test-$timestamp
	# Save some system information
	uname -a > kernel.txt
	cat /proc/cpuinfo > cpuinfo.txt
	cat /proc/meminfo > meminfo.txt

	echo Benchmark initiated at $(date +%Y%m%d-%H%M)
	JVM_args1="-server -Xmx256g -Xmx256g -XX:+UseG1GC -javaagent:$HOME/Documents/briskstream/BriskBenchamrks/lib/classmexer.jar"
	JVM_args2="-server -Xmx1000g -Xmx2000g -XX:+UseG1GC -javaagent:$HOME/Documents/briskstream/BriskBenchamrks/lib/classmexer.jar"
	

for w in 2 #4 8 16 32 64
	do
		#tuple size less than 32 is most likely hit in local after remote fetch.
		for t in 32 #16 8 4 2 #1 2 4 8 16 32 64 128 256 512 1024 #2048 4096 8192 16384
		do	
			#Profiling.
			echo "profiling phase" "window:" $w "tuple size" $t "repeate:" $repeate
			java $JVM_args2 -jar $HOME/Documents/briskstream/BriskBenchamrks/target/Brisk-1.0.0-jar-with-dependencies.jar --THz 30000 --runtime 800 --profile --size_tuple $t --window $w --loop 1000 -mp $HOME/briskstream/test-$timestamp --JVM 2000 -a "wordcount" >> test$t\_$w.txt			
			cat test$t\_$w.txt | grep "finished"
			#Testing. 
			for num_socket in 8 4 2 1 
			do
				for num_cpu in 8 4 2 1
				do 
					for ct1 in 32 16 8 4 2 1 
					do
						for ct2 in 32 16 8 4 2 1 
						do
							for ct3 in 32 #16 8 4 2 1 
							do																			
								echo "native execution:" "num_socket" $num_socket "num_cpu" $num_cpu "window:" $w "tuple size" $t "ct1:" $ct1 "ct2:" $ct2 "ct3:" $ct3 "repeate:" $repeate
								java $JVM_args2 -jar $HOME/Documents/briskstream/BriskBenchamrks/target/Brisk-1.0.0-jar-with-dependencies.jar --num_socket $num_socket --num_cpu $num_cpu --THz 30000 --runtime 350  -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -l --size_tuple $t --window $w --loop 1000 -mp $HOME/briskstream/test-$timestamp --JVM 2000 --native -a "wordcount" >> native$t\_$w\_$num_socket\_$num_cpu.txt	

								echo "Optimize execution:" "num_socket" $num_socket "num_cpu" $num_cpu  "window:" $w "tuple size" $t "ct1:" $ct1 "ct2:" $ct2 "ct3:" $ct3 "repeate:" $repeate
								java $JVM_args2 -jar $HOME/Documents/briskstream/BriskBenchamrks/target/Brisk-1.0.0-jar-with-dependencies.jar --num_socket $num_socket --num_cpu $num_cpu --THz 30000 --runtime 350  -ct1 $ct1 -ct2 $ct2 -ct3 $ct3 -l --size_tuple $t --window $w --loop 1000 -mp $HOME/briskstream/test-$timestamp ---relax 1 --JVM 2000 -a "wordcount" >> aware$t\_$w\_$num_socket\_$num_cpu.txt									
							done
						done
					done
					cat native$t\_$w\_$num_socket\_$num_cpu.txt | grep "finished"
					cat aware$t\_$w\_$num_socket\_$num_cpu.txt | grep "finished"				
				done
			done	
		done 
	done
done	