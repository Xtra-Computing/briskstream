#!/usr/bin/python2
import numpy
result = [numpy.random.bytes(1024*1024) for lon in xrange(1024)]
print len(result)
