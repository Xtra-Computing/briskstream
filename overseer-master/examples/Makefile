inchpc = ../src/hpcOverseer
flags = -m64 -O2 -I$(inchpc) -L../src/hpcOverseer ../src/hpcOverseer/libhpcOverseer.so.1.0.0

all : 
	g++ $(flags) -o test test.cpp
	g++ $(flags) -o test_mt test_mt.cpp -lpthread
	g++ $(flags) -o test_arch test_arch.cpp
	javac -sourcepath ../src/java/overseer.jar java_hpc.java
	javac -sourcepath ../src/java/overseer.jar java_arch.java
	javac -sourcepath ../src/java/overseer.jar java_agent.java
	javac -sourcepath ../src/java/overseer.jar java_ipmi.java

# Housekeeping:
.PHONY: clean
clean :
	-rm -f *.o
	-rm -f test
	-rm -f test_mt
	-rm -f test_arch
	-rm -f *.class

