BriskStream
===

## What?
This project aims at building scalable data stream processing system on modern multicore architectures, with an API similar to Storm (Heron).

## Why?
One of the trend in computer architecture is putting more and more processors into a single server.
Both computing power and memory sizes are becoming larger and larger that in many cases has its superiority than conventional cluster settings.
However, state-of-the-art data streaming systems (e.g., Storm, Heron, Flink) are designed for scale-out.
They underutilize scale-up server seriously as observed by our careful benchmarking.

## How to start
1. Clone the project.

2. Make sure all the dependencies are installed. 

- Overseer 

Please follow the instruction in https://github.com/msteindorfer/overseer to compile overseer.

Copy the compiled overseer.jar (overseer/src/java/overseer.jar) into briskstream/common/lib/ and call "mvn validate" to install the library.

If you don't have sudo, you have to install all the corresponding dependencies in your home directory, and modify the reference path accordingly.

- Other Dependencies: numactl, classmexer 

3. Install the project

 3.1 If you are using IDE (e.g., IntelliJ), simply load the project as maven project.
Note that, if you want to enable more modules (e.g., StormBenchmark), you only need to uncomment them in the root pom.xml. 

 3.2. Alternatively, install in Linux shell as follows.

```
mvn install -DskipTests
```

4. Run application

There are a few options to test and debug the system. 

The simple way is to load the project using Intellij IDEA, and set Runner (BriskRunner, FlinkRunner, etc.) as the main class file.

To test the system on Linux server, you need to install the project (Step 3) and run the compiled jar package.
You may also use the prepared scripts found in common/scripts to launch the program with predefined configurations. 

### IMPORTANT NOTE

1. Use ``mvn install -P cluster -DskipTests`` to install Strombenchmark on server. This will prevent the duplicate configuration file issue.
2. The datasets are missing in the repo. You can use your own datasets. Some of the datasets I used can be found here https://github.com/mayconbordin/storm-applications

## Project Status
BriskStream is still under heavily active development, expect more bug-fixing and more advance features (e.g., transactional state management, elasticity, fault-tolerance, etc.).

The original commit history of briskstream can be found at https://bitbucket.org/briskStream/briskstream/src/Brisk/, where you may find earlier version of BriskStream. This may help you to understand the project better.

### Branches

- stable: the branch to test BriskStream with NUMA-aware optimization.
- TStream: a variant of BriskStream that supports concurrent stateful stream processing.
- others: under development.


## How to Cite BriskStream

If you use BriskStream in your paper, please cite our work.

* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on Multicore Architectures, SIGMOD, 2019

* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on Multicore Processors, ICDE, 2020

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

## Some Implementation Design Decisions Notes
### Partition Controller
Each executor owns a partition controller (PC), i.e., Shuffle PC or Fields PC etc, this PC maintain the output status, and can be used to support customised partition control.
### Tuple Scheduler
Each executor may need to fetch from multiple different input queues (Multiple stream, multiple operator and multiple executors). The sequence of tuple fetching may affect the final processing latency of a tuple. The system now support two types of scheduling strategy: 1) sequential (default config), which sequentially looks through each stream, each operator and each executor; 2) uniform, which gives equal chance of look up at different input channel.
### Stream Processing Model
There are two popular stream processing model nowadays: 1) Discretized stream processing model and 2) Continuous stream processing model.
We select the later one because we aim at building a stream processing system with a truely streaming engine (like Flink).

