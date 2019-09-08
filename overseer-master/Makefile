# Overseer suite, native code installer
# Achille Peternier (C) 2010 USI

# Change this according to your system:
libdir = /usr/local/lib


# Don't modify after this line:
.PHONY: install all clean

all:
	cd src/hpcOverseer; \
        make
	cd src/hpcOverseerWrapper; \
	make
	cd src/overAgent; \
	make
	cd src/overIpmi; \
	make
	cd src/java; \
	make
	cd examples; \
	make

clean:
	cd src/hpcOverseer; \
        make clean
	cd src/hpcOverseerWrapper; \
	make clean
	cd src/overAgent; \
	make clean
	cd src/overIpmi; \
	make clean
	cd src/java; \
	make clean
	cd examples; \
	make clean

install:	
	cp src/hpcOverseer/libhpcOverseer.so.1.0.0 $(libdir)
	ln -sf libhpcOverseer.so.1.0.0 $(libdir)/libhpcOverseer.so
	ln -sf libhpcOverseer.so.1.0.0 $(libdir)/libhpcOverseer.so.1
	cp src/hpcOverseerWrapper/libhpcOverseerWrapper.so.1.0.0 $(libdir)
	ln -sf libhpcOverseerWrapper.so.1.0.0 $(libdir)/libhpcOverseerWrapper.so
	ln -sf libhpcOverseerWrapper.so.1.0.0 $(libdir)/libhpcOverseerWrapper.so.1
	cp src/overAgent/liboverAgent.so.1.0.0 $(libdir)
	ln -sf liboverAgent.so.1.0.0 $(libdir)/liboverAgent.so
	ln -sf liboverAgent.so.1.0.0 $(libdir)/liboverAgent.so.1
	cp src/overIpmi/liboverIpmi.so.1.0.0 $(libdir)
	ln -sf liboverIpmi.so.1.0.0 $(libdir)/liboverIpmi.so
	ln -sf liboverIpmi.so.1.0.0 $(libdir)/liboverIpmi.so.1
	/sbin/ldconfig

