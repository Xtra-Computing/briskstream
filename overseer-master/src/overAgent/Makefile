# overAgent
# Achille Peternier (C) 2010 USI

#################################
# Edit this part (if necessary) #
#################################

incjni = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux



########################################
# Don't touch anything after this line #
########################################

# Variables:
flags = -m64 -O2 -fPIC -c $(incjni) -I../hpcOverseer

# Creation of liboverAgent:
liboverAgent.so.1.0.0 : overAgent.o threadList.o jniInterface.o 
	g++ -shared -m64 -Wl,-soname,liboverAgent.so.1.0.0 -o liboverAgent.so.1.0.0 overAgent.o threadList.o jniInterface.o -lc

overAgent.o : overAgent.cpp
	g++ $(flags) overAgent.cpp

threadList.o : threadList.cpp
	g++ $(flags) threadList.cpp

jniInterface.o : jniInterface.cpp
	g++ $(flags) jniInterface.cpp


# Housekeeping:
.PHONY: clean
clean :
	-rm -f *.o *.so.*
