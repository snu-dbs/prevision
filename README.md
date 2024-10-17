# PreVision: An Out-of-Core Matrix Computation System with Optimal Buffer Replacement

This README describes how to reproduce the paper named "PreVision: An Out-of-Core Matrix Computation System with Optimal Buffer Replacement".
If you have any questions about it, please [contact us](mailto:koo@dbs.snu.ac.kr).
We will be happy to hear that and respond to you joyfully.

This project contains three major parts: PreVision source codes, evaluation scripts for all systems, and a data generator.
The source codes and scripts are tuned for the current directory structure.
Thus, please do not change the directory structure.
The project structure is as follows.

```
.
├── README.md                   # This File
├── plot                        # Scripts for Plotting
├── buffertile                  # PreVision Buffer Manager
├── lam_executor                # PreVision Executor
├── linear_algebra_module       # PreVision Linear Algebra Operators
├── evaluation                  # Evaluation Scripts
├── makeall.sh                  # Build PreVision. See `./evaluation/prevision/exec_eval`.
├── slab-benchmark              # Data Generator (from the SLAB Benchmark: https://adalabucsd.github.io/slab.html)
├── tilechunk                   # PreVision Buffer Wrapper
└── tilestore                   # PreVision I/O Manager
```

Now we are going to go through the below steps.
1. Install prerequisites 
2. Data generation
3. Data loading for some systems and evaluation

## Install Prerequisites

We recommend using Ubuntu as the operating system.
We have tested on Ubuntu 18.04 and 20.04.

Since some systems throw an out-of-memory error when preparing datasets, we recommend using a machine with a large memory size.
Also, since the size of datasets that will be generated for the evaluation is quite huge, please make sure that you have at least 3 terabytes of free disk space.

Before getting started, you need to install comparison systems.
This is required since every later step depends on the systems.

### SystemDS

Please install Java 11 before installing SystemDS because the system requires it.

Download [SystemDS 3.1.0](https://archive.apache.org/dist/systemds/3.1.0/systemds-3.1.0-bin.tgz) and un-compress it to the current directory (the root directory of the above directory structure).
Please make sure the following.

- Add the absolute path of `./systemds-3.1.0-bin/bin/` to the `PATH` environmental variable.
- Add the absolute path of `./systemds-3.1.0-bin/` to the `SYSTEMDS_ROOT` environmental variable.

### MLlib (Spark)

Download [Spark 3.3.2](https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz) and un-compress it to the current directory.
Please make sure that add the absolute path of `./spark-3.3.2-bin-hadoop3/bin/` to the `PATH` environmental variable.

### MADlib

MADlib is a machine learning extension of PostgreSQL.
Thus, you need to install PostgreSQL first, and then install MADlib.

Install PostgreSQL 12.14 with plpython support ([`--with-python`](https://www.postgresql.org/docs/current/install-make.html#CONFIGURE-OPTION-WITH-PYTHON)) on your computer.
After that, following [the MADlib installation guide](https://cwiki.apache.org/confluence/display/MADLIB/Installation+Guide), install MADlib 1.21.0 with schema `madlib`.
If you install MADlib from source code, please make sure the Postgres installation is detected during initializing a cmake build.

### SciDB

Since SciDB changed to closed-source software, the latest version we can use is 19.11.
Fortunately, a well-structured docker image for SciDB 19.11 is published on the Internet, so we can use it.
You can find the docker image at [here](https://hub.docker.com/r/rvernica/scidb/tags).

Please refer to the below to set up SciDB:

- If you run SciDB using docker without setting a volume, every imported data will be stored in the docker directory (e.g., `/var/lib/docker/`).
If you do not have enough disk space for that directory, consider using a volume to store SciDB data outside the docker directory. 

- If you use docker, make sure that the shared memory threshold is enough.
If a container has a limited shared memory size, SciDB may raise a memory error.
You can use the `--shm-size` option for the `docker run` command (e.g., `--shm-size=30gb`).
- You must use a normal user (not a root user) to run SciDB. If you run SciDB using the root account, SciDB would make an MPI error. 

Here is an **example** of running a SciDB container.
```bash
sudo docker run --name prevision-scidb-exp -it --shm-size=30gb -v /prevision/slab-benchmark/prevision:/prevision -v /prevision/evaluation/scidb/dbpath:/dbpath rvernica/scidb:19.11
```

SciDB requires at least two instances that one for a coordinator and the other for executors.
So please use additional one instance for all SciDB experiments.


## Data Generation

Now, we are going to generate matrices for experiments.
Please move your directory to `./slab-benchmark/prevision/` and open `README.md` there.

## Evaluation

Implementations and scripts for evaluation are stored in the `evaluation` directory.
Please refer to the `/evaluation/README.md`.

## Plotting

Scripts for plotting are stored in the `plot` directory.
Please refer to the `./plot/README.md`.
