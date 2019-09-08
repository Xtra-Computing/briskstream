# java bindings (overseer.jar)
# Achille Peternier (C) 2010 USI

#
# Make sure that the java/bin directory is in the path
# Fix the javac and jar paths otherwise
#

# Creation of overseer.jar:
all : 
	javac ch/usi/overseer/OverHpc.java
	javac ch/usi/overseer/OverAgent.java
	javac ch/usi/overseer/OverIpmi.java
	jar cf overseer.jar ch/usi/overseer/*

# Housekeeping:
.PHONY: clean
clean :
	-rm -f overseer.jar
	-rm -f ch/usi/overseer/*.class
