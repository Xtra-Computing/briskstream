#!/bin/bash
HOME="/home/flink"
JAVA_HOME="$HOME/Documents/jdk1.8.0_77"
#bash ./generate.sh

searchdir="--search-dir all:rp=$HOME/flink-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=$HOME/Documents/flink-1.0.2/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=/home/tony/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"
#WC(4),FD,LG,SD,VS,TM,LR(10)
app=4
app_end=10
while [ $app -le $app_end ]
do
        case $app in
				4)
                path=vtune/4/1/4_1_1_1/resource
                result_path=./Results/4
				app_name=wc
                ;;
                5)
                path=vtune/5/1/4_1_1/resource
                result_path=./Results/5
				app_name=fd
                ;;
                6)
                path=vtune/6/1/4_1_1/resource
                result_path=./Results/6
				app_name=lg
                ;;
                7)
                path=vtune/7/1/4_1_1_1/resource
                result_path=./Results/7
				app_name=sd
                ;;
                8)
                path=vtune/8/1/4_1_1_1/resource
                result_path=./Results/8
				app_name=vs
                ;;
                9)
                path=vtune/9/1/4_1_1_1/resource
                result_path=./Results/9
				app_name=tm
                ;;
                10)
                path=vtune/10/1/4_1_1_1_1/resource
                result_path=./Results/10
				app_name=lr
                ;;
        esac

	cat $result_path/$app_name.lst | \
	while  read CMD; do
		fun=$CMD
		echo $fun
		if [ ! -f $result_path/$fun.csv ] ; then
		amplxe-cl --report hw-events -source-object function="$fun" -group-by address  -csv-delimiter comma -r $path/resource.amplxe  $searchdir > $result_path/$fun.csv
		fi
	done
let app=$app+1
done
