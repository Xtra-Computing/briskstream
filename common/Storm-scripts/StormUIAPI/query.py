import stormuiapi
import sys

storm = stormuiapi.StormUIAPI(stormhost="localhost", stormport=8080)
#print storm.getClusterConfiguration()
print storm.getTopologyByName(sys.argv[1])
print sys.argv[1]
