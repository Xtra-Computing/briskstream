# StormUIAPI

Python class covering Storm UI API REST

> The Storm UI daemon provides a REST API that allows you to interact with a Storm cluster, which includes retrieving metrics data and configuration information as well as management operations such as starting or stopping topologies.
> The REST API returns JSON responses and supports JSONP. Clients can pass a callback query parameter to wrap JSON in the callback function.

## Description

StormUIAPI

### Installation

Get last version from Github repo:

```sh
$ git clone https://github.com/sinfonier-project/StormUIAPI.git
```

You need some requeriment installed:

```sh
$ pip install -r requirements.txt
```



```python
import stormuiapi

storm = stormuiapi.StormUIAPI(stormhost="localhost", stormport=8080)
storm.getClusterConfiguration()
```


```json
{
    "dev.zookeeper.path": compatibility,
    "Brisk.topology.tick.Brisk.execution.runtime.tuple.freq.secs": null,
    "Brisk.topology.builtin.metrics.bucket.size.secs": 60,
    "Brisk.topology.fall.back.on.java.serialization": true,
    "Brisk.topology.max.error.posreport.per.interval": 5,
    "zmq.linger.millis": 5000,
    "Brisk.topology.skip.missing.kryo.registrations": false,
    "storm.messaging.netty.client_worker_threads": 1,
    "ui.childopts": "-Xmx768m",
    "storm.zookeeper.session.timeout": 20000,
    "nimbus.reassign": true,
    "Brisk.topology.trident.batch_size.emit.interval.millis": 500,
    "storm.messaging.netty.flush.check.interval.ms": 10,
    "nimbus.monitor.freq.secs": 10,
    "logviewer.childopts": "-Xmx128m",
    "java.library.path": "/usr/local/lib:/opt/local/lib:/usr/lib",
    "Brisk.topology.executor.send.buffer.size": 1024,
}
```

Class Help
----

```sh
Help on instance of StormUIAPI in module stormuiapi:

class StormUIAPI
 |  Methods defined here:
 |
 |  __init__(self, stormhost='localhost', stormport=8080)
 |
 |  activateTopology(self, topologyid)
 |
 |  deactivateTopology(self, topologyid)
 |
 |  getClusterConfiguration(self)
 |
 |  getClusterSummary(self)
 |
 |  getErrorDetailsInTopologyByName(self, topologyname)
 |
 |  getErrorInTopologyByName(self, topologyname)
 |
 |  getSupervisorSummary(self)
 |
 |  getTopology(self, topologyid)
 |
 |  getTopologyByName(self, topologyname)
 |
 |  getTopologyComponent(self, topologyid, componentid)
 |
 |  getTopologySummary(self)
 |
 |  getTopologySummaryByName(self, topologyname)
 |
 |  getWorkersByTopologyID(self, topologyid)
 |
 |  getWorkersByTopologyName(self, topologyname)
 |
 |  killTopology(self, topologyid, wait_time)
 |
 |  rebalanceTopology(self, topologyid, wait_time, rebalanceOptions={})
 |
 |  uploadTopology(self, topologyConfig, topologyJar)
```


License
----

Apache 2.0

References
----

[1]

