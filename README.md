# BriskStream
## What?
This project aims at building a non-distributed data stream processing system, with an API similar to Storm.
## Why?
One of the trend in computer architecture is putting more and more processors into a single server.
Both computing power and memory sizes are becoming larger and larger that in many cases has its superiority
than conventional cluster settings.
However, state-of-the-art data streaming systems (e.g., Storm, Heron, Flink) are designed for scale-out.
They underutilize scale-up server seriously as observed by our careful benchmarking.
## How?
The first step is to build a data stream processing system similar to Storm without network-related component,
e.g., serialization.
If you are interested in this idea, please contact me and join this project!

## For contributors
First of all, thanks for contributing! 
I highlight here a few design strategies that please bear in mind.
### Lightweight critical path
For example, we should always try to reduce the number of referencing to common memory structure during executor execution.
### Compatible API of Storm
This project should provide compatible API of Storm for easy comparison, and project migration.

##Design decisions
###Partition Controller
Each executor owns a partition controller (PC), i.e., Shuffle PC or Fields PC etc, this PC maintain the output status, and can be used to support customised partition control.
###Tuple Scheduler
Each executor may need to fetch from multiple different input queues (Multiple stream, multiple operator and multiple executors). The sequence of tuple fetching may affect the final processing latency of a tuple. The system now support two types of scheduling strategy: 1) sequential, which sequentially looks through each stream, each operator and each executor; 2) uniform, which gives equal chance of look up at different input channel.
###Stream processing model
There are two popular stream processing model nowadays: 1) Discretized stream processing model and 2) Continuous stream processing model.
We select the later one because our target is to minimize process latency, yet we sacrify other things: fault tolerence, scale-out capability and so on.

##Install and Dependencies
###Overseer
sudo install -y libpfm hwloc freeipmi libpfm-devel hwloc-devel freeipmi-devel
make
sudo make install

After that, you have to copy the overseer.jar into briskstream/common/lib/ and call ``mvn validate" to install the library.

If you don't have sudo, you have to install all the corresponding dependencies in your home directory, and modify the reference path accordingly.
###Others
numactl

###Version
The current version of BriskStream is 1.2
