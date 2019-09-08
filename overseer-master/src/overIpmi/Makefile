# overIpmi
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

# Creation of liboverIpmi:
liboverIpmi.so.1.0.0 : overIpmi.o 
	g++ -shared -m64 -Wl,-soname,liboverIpmi.so.1.0.0 -o liboverIpmi.so.1.0.0 overIpmi.o -lipmimonitoring -lc

overIpmi.o : overIpmi.c
	gcc $(flags) overIpmi.c

# Housekeeping:
.PHONY: clean
clean :
	-rm -f *.o *.so.*
