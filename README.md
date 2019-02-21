# BriskStream
## What?
This project aims at building scalable data stream processing system on modern multicore architectures, with an API similar to Storm (Heron).

## Why?
One of the trend in computer architecture is putting more and more processors into a single server.
Both computing power and memory sizes are becoming larger and larger that in many cases has its superiority
than conventional cluster settings.
However, state-of-the-art data streaming systems (e.g., Storm, Heron, Flink) are designed for scale-out.
They underutilize scale-up server seriously as observed by our careful benchmarking.

## Some Implementation Design Decisions
### Partition Controller
Each executor owns a partition controller (PC), i.e., Shuffle PC or Fields PC etc, this PC maintain the output status, and can be used to support customised partition control.
### Tuple Scheduler
Each executor may need to fetch from multiple different input queues (Multiple stream, multiple operator and multiple executors). The sequence of tuple fetching may affect the final processing latency of a tuple. The system now support two types of scheduling strategy: 1) sequential (default config), which sequentially looks through each stream, each operator and each executor; 2) uniform, which gives equal chance of look up at different input channel.
### Stream Processing Model
There are two popular stream processing model nowadays: 1) Discretized stream processing model and 2) Continuous stream processing model.
We select the later one because we aim at building a stream processing system with a truely streaming engine (like Flink).

## Install and Dependencies
### Overseer
sudo install -y libpfm hwloc freeipmi libpfm-devel hwloc-devel freeipmi-devel
make
sudo make install

After that, you have to copy the overseer.jar into briskstream/common/lib/ and call "mvn validate" to install the library.

If you don't have sudo, you have to install all the corresponding dependencies in your home directory, and modify the reference path accordingly.
### Other Dependencies
numactl
classmexer

## Project Status
BriskStream is still under active development, expect more bug-fixing and more advance features (e.g., transactional state management, elasticity, fault-tolerance, etc.).

The original commit history of briskstream can be found at https://bitbucket.org/briskStream/briskstream/src/Brisk/, where you may find earlier version of BriskStream. This may help you to understand the project better.

## How to Cite BriskStream

If you use BriskStream in your paper, please cite our work.

```
@article{zhangbriskstream19,
 author = {Zhang, Shuhao and He, Jiong and Zhou, Chi (Amelie) and He, Bingsheng},
 title = {BriskStream: Scaling Stream Processing on Multicore Architectures},
 booktitle = {Proceedings of the 2019 International Conference on Management of Data},
 series = {SIGMOD '19},
 year={2019},
 isbn = {978-1-4503-5643-5},
 url = {https://doi.acm.org/10.1145/3299869.3300067},
 doi = {10.1145/3299869.3300067},
}
```


### Other related publications

* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data stream processing systems on multi-core processors, ICDE, 2017 (code: https://github.com/ShuhaoZhangTony/ProfilingStudy)

