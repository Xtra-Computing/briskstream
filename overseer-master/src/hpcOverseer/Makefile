# hpcOverseer
# Achille Peternier (C) 2010 USI



########################################
# Don't touch anything after this line #
########################################

# Variables:
flags = -m64 -O2 -fPIC -c

# Creation of libhpcOverseer:
libhpcOverseer.so : hpcOverseer.o hpcOverseer_cpuinfo.o hpcOverseer_rdtsc.o hpcOverseer_scheduler.o hpcOverseer_tools.o
	g++ -shared -m64 -Wl,-soname,libhpcOverseer.so.1.0.0 -o libhpcOverseer.so.1.0.0 hpcOverseer.o hpcOverseer_cpuinfo.o hpcOverseer_rdtsc.o hpcOverseer_scheduler.o hpcOverseer_tools.o -lc -lpfm -lhwloc

hpcOverseer.o : hpcOverseer.cpp
	g++ $(flags) hpcOverseer.cpp

hpcOverseer_cpuinfo.o : hpcOverseer_cpuinfo.cpp
	g++ $(flags) hpcOverseer_cpuinfo.cpp
   
hpcOverseer_rdtsc.o : hpcOverseer_rdtsc.cpp
	g++ $(flags) hpcOverseer_rdtsc.cpp

hpcOverseer_scheduler.o : hpcOverseer_scheduler.cpp
	g++ $(flags) hpcOverseer_scheduler.cpp

hpcOverseer_tools.o : hpcOverseer_tools.cpp
	g++ $(flags) hpcOverseer_tools.cpp

# Housekeeping:
.PHONY: clean
clean :
	-rm -f *.o *.so.*


