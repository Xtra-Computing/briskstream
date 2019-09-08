# hpcOverseerWrapper
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

# Creation of libhpcOverseerWrapper:
libhpcOverseerWrapper.so.1.0.0 : hpcOverseerWrapper.o 
	g++ -shared -L../hpcOverseer -m64 -Wl,-soname,libhpcOverseerWrapper.so.1.0.0 -o libhpcOverseerWrapper.so.1.0.0 hpcOverseerWrapper.o ../hpcOverseer/libhpcOverseer.so.1.0.0 -lc

hpcOverseerWrapper.o : hpcOverseerWrapper.cpp ch_usi_overseerJNI_WrapperJNI.h
	g++ $(flags) hpcOverseerWrapper.cpp

# Housekeeping:
.PHONY: clean
clean :
	-rm -f *.o *.so.*
