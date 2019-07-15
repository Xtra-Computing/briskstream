@echo off
setlocal enabledelayedexpansion enableextensions
REM bt = -1 is for micro-applications only
REM set /a "bt = -1"

set /a "USE_LARGEPAGE = 0"

set /a "JIT = 0"
set /a "num_workers = 1"
set /a "num_workers_end = 1"

set /a "bt = 0"
set /a "bt_end = -1"
:forloop_batch
set /a "x = 0"
set /a "x_end = 0"
:forloop_x
REM set /a "vm = 384/num_workers"
set /a "vm = 128/num_workers"
REM set /a "vm = 256/num_workers"
:forloop_vm
REM app from 4:9 is ok; 10 is discarding. 11 is strange; 12 is broken;
REM WC(4),FD,LG,SD,VS,TM(9)
set /a "app = 4"
set /a "app_end = 4"
:forloop_app
set /a "c = 8"
set /a "c_end = 8"
:forloop_bolt

set /a "cpu_counter = 2"
set /a "cpu_counter_end = 1"

if %app%==0 (
	set count_number=2
	REM set c=6
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_cache\%num_workers%_%c%_!count_number!_%bt%_small
)
if %app%==1 (
	set count_number=2
	REM set c=6
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_mem\%num_workers%_%c%_!count_number!_%bt%_small
)
if %app%==2 (
	set count_number=2
	REM set c=5
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_communicate\%num_workers%_%c%_!count_number!_%bt%_small
	
)
if %app%==3 (
	set count_number=2
	REM set c=48
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_compute\%num_workers%_%c%_!count_number!_%bt%_small
)

if %app%==5 (
	set count_number=100
	
 	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_fraud-detection\%num_workers%_!count_number!_%bt%_small
)
if %app%==4 (
	set count_number=4
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_word-count\%num_workers%_!count_number!_%bt%_small
)
if %app%==6 (
	set count_number=2
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_log-processing\%num_workers%_!count_number!_%bt%_small
)
if %app%==8 (
	set count_number=1
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_voipstream\%num_workers%_!count_number!_%bt%_small
)

if %app%==7 (
	set count_number=4

	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_spike-detection\%num_workers%_!count_number!_%bt%_small
)

if %app%==9 (
	set count_number=1
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_traffic-monitoring\%num_workers%_!count_number!_%bt%_small_%c%
)
if %app%==10 (
	set count_number=100
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_click-analytics\%num_workers%_!count_number!_%bt%_small
)
if %app%==11 (
	REM set /a "count_number = 1" // cannot repeat!
	set count_number=1
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_machine-outlier\%num_workers%_!count_number!_%bt%_small
)
if %app%==12 (
	set count_number=1
	
	set MY_PATH2=C:\Users\szhang026\Documents\apache-storm-1.0.0\output_sentiment-analysis\%num_workers%_!count_number!_%bt%_small
)

mkdir %MY_PATH2%
if %app%==0 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count-cache -n %num_workers% -bt %bt% -ct %c% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)
if %app%==1 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count-m -n %num_workers% -bt %bt% -ct %c% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)
if %app%==2 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count-com -n %num_workers% -bt %bt% -ct %c% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)
if %app%==3 (	
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count-c -n %num_workers% -bt %bt% -ct %c% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)

if %app%==4 (
	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages"
		REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count-n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=1000000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a word-count -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
		)
	)
)
if %app%==5 (

	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a fraud-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a fraud-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=10000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a fraud-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
			REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a fraud-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
		)		
	)
)
if %app%==6 (
	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a executionNode-processing -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a executionNode-processing -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=10000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a executionNode-processing -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
			REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a executionNode-processing -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
		)	
	)	
)

if %app%==7 (
	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a spike-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a spike-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=10000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (		
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a spike-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
			REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a spike-detection -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
		)		
	)	
)

if %app%==8 (
	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a voipstream -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a voipstream -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=10000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a voipstream -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
			REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a voipstream -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
		)		
	)
)

if %app%==9 (
	if %USE_LARGEPAGE%==1 (
		start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a traffic-monitoring -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
	) else (
		if %JIT%==1 (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a traffic-monitoring -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:CompileThreshold=10000 -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly -XX:LogFile=%MY_PATH2%\jit.executionNode"
		) else (
			start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a traffic-monitoring -ct %c% -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
			REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a voipstream -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
		)
	)		
)

if %app%==10 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a click-analytics -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
	REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a click-analytics -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+PrintGCTimeStamps -Xloggc:%MY_PATH2%\gc.executionNode"
	REM start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a click-analytics -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -XX:+UseLargePages"
)

if %app%==11 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a machine-outlier -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)

if %app%==12 (
	start storm.cmd jar C:\Users\szhang026\Documents\test.jar storm.applications.BriskRunner mytest -a sentiment-analysis -n %num_workers% -bt %bt% -mp %MY_PATH2% -m remote -cn %count_number% -co "-server -Xmx%vm%g -Xms%vm%g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
)

IF %x% gtr 0 (
	del "%MY_PATH2%\spout_threadId.txt"
	:forloops	
	TIMEOUT /T 30 /NOBREAK
	for %%R in ("%MY_PATH2%\spout_threadId.txt") do if not %%~zR gtr 1 goto forloops
	set /p r=< "%MY_PATH2%\spout_threadId.txt" 

	"C:\Java\bin\jstack.exe" %r% >> "%MY_PATH2%\threaddump_%x%.txt
)


REM IF %x%==1 (
	REM concurrency has some problem, need to mannualy..
	REM "C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect concurrency -target-duration-type=medium -data-limit=150 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10 --target-pid %r% -result-dir %MY_PATH2%\concurrency >> %MY_PATH2%\profile1.txt	
REM )

IF %x%==1 (
	REM General Exploration with CPU concurrency and Memory Bandwidth
	"C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10 --target-pid  %r% -result-dir %MY_PATH2%\resource	>> %MY_PATH2%\profile1.txt
)

IF %x%==2 (
	REM general
	REM "C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003 -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10  --target-pid  %r% -result-dir %MY_PATH2%\general >> %MY_PATH2%\profile2.txt
	
	REM "C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003 -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10  --target-pid  %r% -result-dir %MY_PATH2%\general_%c% >> %MY_PATH2%\profile2_%c%.txt	
	
	"C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003 -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10  --target-pid  %r% -result-dir %MY_PATH2%\general_%c% >> %MY_PATH2%\profile2_%c%.txt
	
)

IF %x%==3 (
	REM context switch
	REM "C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect-with runsa -knob enable-stack-collection=true -knob enable-call-counts=true -knob enable-trip-counts=true -knob enable-frames=false -knob enable-context-switches=true -knob preciseMultiplexing=true --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10 --duration 500 --target-pid %r% -result-dir %MY_PATH2%\context >> %MY_PATH2%\profile3.txt
	"C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10 --duration 500 --target-pid %r% -result-dir %MY_PATH2%\context >> %MY_PATH2%\profile3.txt
)

IF %x%==4 (
"C:\Program Files (x86)\IntelSWTools\VTune Amplifier XE 2016\bin64\amplxe-cl" -collect-with runsa -knob event-config=OFFCORE_RESPONSE.ALL_DATA_RD.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_CODE_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HITM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HIT_FORWARD_0:sa=100003 -data-limit=0 --search-dir sym:p=C:\Java\bin --search-dir bin:p=C:\Java\bin --start-paused --resume-after 10  --target-pid  %r% -result-dir %MY_PATH2%\MEM_%c% >> %MY_PATH2%\profile4_%c%.txt
)
	
:forloop_sink
TIMEOUT /T 20 /NOBREAK
:forloop_cpu
	TIMEOUT /T 10 /NOBREAK	
	wmic cpu get loadpercentage >> %MY_PATH2%\cpu_utilization.txt
	set /a "cpu_counter = cpu_counter + 1"
if %cpu_counter% LEQ %cpu_counter_end% goto forloop_cpu	
for %%R in ("%MY_PATH2%\sink.txt") do if not %%~zR gtr 1 goto forloop_sink

set /a "c = c *2 "
if %c% LEQ %c_end% goto forloop_bolt

set /a "app = app + 1"
if %app% LEQ %app_end% goto forloop_app

REM set /a "num_workers = num_workers + 1"
REM if %num_workers% LEQ %num_workers_end% goto forloop_vm

set /a "x = x + 1"
if %x% LEQ %x_end% goto forloop_x

set /a "bt = bt * 2"
if %bt% LEQ %bt_end% goto forloop_batch
