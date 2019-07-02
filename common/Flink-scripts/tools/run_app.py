# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 16:04:01 2016

@author: szhang026
"""

#! /usr/bin/env python
JAVA_HOME="/home/tony/Documents/jdk1.8.0_77"
USE_LARGEPAGE=0
JIT=0
#vm="384"
vm=128
up=vm/8
num_workers=1
bt=1
bt_end=1
xmx="Xmx%(v)g" % dict(v=vm)
xms="Xms%(v)g" % dict(v=vm)
searchdir="--search-dir all:rp=/home/tony/storm-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=/home/tony/Documents/apache-storm-1.0.0/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=/home/tony/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"

arg="-n %d -bt %d -mp %s -m remote -cn %d" % (num_workers,bt,)
arg1=$arg -co "-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages"
arg2=$arg -co "-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -javaagent:/home/tony/Documents/intrace-agent.jar"
arg3=$arg -co "-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
echo $arg1



def hello_command(name, print_counter=False, repeat=10):
    """Print nice greetings."""
    for i in range(repeat):
        if print_counter:
            print i+1,
        print 'Hello, %s!' % name

if __name__ == '__main__':
    import scriptine
    scriptine.run()