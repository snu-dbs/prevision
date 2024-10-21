# Data Loading and Evaluation

This README describes how to load generated datasets and run experiments on each system.

The directory structure is as follows.
Each subdirectory of this directory is responsible for each comparison system.
For detailed instructions, please refer to the next sections.

```
evaluation
├── README.md               # README
├── dask                    # Evaluation for Dask
├── madlib                  # Evaluation for MADlib
├── mllib                   # Evaluation for MLlib
├── numpy_memmap            # Evaluation for NumPy
├── prevision               # Evaluation for PreVision
├── requirements.txt        
├── scidb                   # Evaluation for SciDB
└── systemds                # Evaluation for SystemDS
```

## Prerequisites

Please make sure the following is installed:
- `spark-submit`. you can call it from anywhere
- sbt 1.8.2
- The GNU time command. If not, you can install with `sudo apt install time`.

To install the prerequisite python packages needed for this experiment, run the following commands in the current directory (i.e., `/evaluation/`).

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

Note that the storage device containing the current directory could affect the performance.

We recommend extending the time-out limit for sudo because experiments will run for a long time, and it may require a password for sudo repeatedly.

## Evaluation

We ask reviewers to record the elapsed times of experiments. 
Each script prints elapsed times and those should be recorded manually to compare with the results.

The scripts in this directory are configured to run on a machine with 32GB memory (settings presented in the paper).
So please update memory configurations for systems if you want different memory setting.

### NumPy

The script for NumPy is located at `./numpy_memmap/exp.sh`.
The `exp.sh` will run all experiments presented in the paper.
Note that the script will copy some npy files to the current directory and run with these data.

The usage of the script is as follows.
If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp.sh` script to correctly direct the dataset path.

```bash
# current directory: /evaluation/numpy_memmap/
# make sure that we are on venv
bash exp.sh
```

### Dask

The Dask evaluation script is located at `./dask/exp_alg.sh`.
The `exp_alg.sh` will run all experiments presented in the paper.

The usage is as follows.
If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp_alg.sh` script to correctly direct the dataset path.

```bash
# current directory: /evaluation/dask/
# make sure that we are on venv
bash exp_alg.sh
```

### SciDB

#### Files
The `./scidb/guest/` contains SciDB queries, data loaders, and configuration files that we used.
The `load-dense.sh`, `load-sparse.sh`, and `load-pagerank.sh` are data-loading scripts.
You may need to modify the `DIR` variable of each script to adjust the dataset directory.
The `alg.sh` script defines the matrix computation queries.
The `setup.sh` and `clean.sh` are scripts that needed to be executed before and after experiments, respectively.

#### Configuration

The `config.ini` file is a configuration we used for non-parallelism experiments.
Please adjust the number of instances (the last value of `server-0`) when parallelism experiments are required.
For example, if parallelism is four, the `server-0` should be `127.0.0.1,4` (one coordinator and four executors).
After modified, you should load data again.

For the buffer size, the equal-sized values for each instances are set to the `smgr-cache-size` and `mem-array-threshold`.
For instance, set `3000` to each of them if parallelism is four; the total # of instances is 5 so (`smgr-cache-size` + `mem-array-threadhold`) * 5 should be 30GB.
Sometimes it might cause an out-of-memory error because SciDB overuses memory capacity. 
In such a case, lowering memory numbers might make the workload run without an OOM error.

The other configurations should not be modified.
For more information, please refer to [the SciDB configuration documentation](https://paradigm4.atlassian.net/wiki/spaces/scidb/pages/3395882557/Configuring+SciDB).


#### Evaluation

##### For Docker user

Please transfer the script files in the `guest` directory to the docker container.
The commands in the `exp.sh` will be sent to the docker container and execute queries remotely.
Your docker configuration may be different from ours, so please modify the `exp.sh` and `alg-remote.sh` for your environment to use these.

Before running experiments, please run the data loading scripts. 
Please review the `DIR` path in the script files.

```bash
# Current directory: /evaluation/scidb/guest/
bash load-dense.sh
bash load-sparse.sh
bash load-pagerank.sh
```

After that, you can start an evaluation with the following command on the host side.

```bash
# Current directory: /evaluation/scidb/
# Please configure exp.sh and alg-remote.sh and put guest scripts in appropriate directories.
bash exp.sh
```

##### For non-docker user

Before running experiments, please run the data loading scripts. 

```bash
# Current directory: /evaluation/scidb/guest/
bash load-dense.sh
bash load-sparse.sh
bash load-pagerank.sh
```

You can start the evaluation with the following command.

```bash
# Current directory: /evaluation/scidb/guest/
# Note that alg-local.sh and alg.sh should be placed in the same directory.
# Note also that the setup.sh and clean.sh will be executed in the alg-local.sh script.
bash alg-local.sh
```

### SystemDS

Please make sure the following.
- You can run `spark-submit` anywhere (i.e., add the spark `bin` directory to your `PATH` environmental variable). It is required to use SystemDS in out-of-core situations.
- You have set the `SYSTEMDS_ROOT` and `SPARK_ROOT` environmental variables.
- You have made SystemDS configuration on `$SYSTEMDS_ROOT/conf/`. We uploaded the configuration we used to `./systemds/SystemDS-config.xml`. Please modify values of `sysds.localtmpdir`, `systemds.scratch`, and `sysds.native.blas.directory` to your environment's one.

Before running an experiment, please move to `./systemds/dense` or `./systemds/sparse` directory depending on the experiment.
Then, open the `./src/main/scala/systemds_ml_algorithms.scala` file.
You can find `ml.setConfig()` statement near line 26 (for both dense and sparse cases).
Please make sure that the config file name described in the line is the same as yours.
If it is not the same, please update it.
After that, go back to the `dense` or `sparse` directory depending on the experiment.

Please note that the LR task uses OpenBLAS, whereas the NMF task does not. You can find the `ml.setConfigProperty()` statements in lines 53 and 56 in the `./src/main/scala/systemds_ml_algorithms.scala` file. This configuration is necessary because SystemDS produces incorrect results during the NMF task when multiplying a matrix by its transpose (specifically, computing $HH^T$).

Since SystemDS relies on Spark for out-of-core computation, its performance is highly dependent on the configuration of Spark. The best-performed memory configurations from our experiments are set as the default. But, you can manually set both the driver and executor memory by passing them as arguments to `lr.sh`, `nmf.sh`, or `pagerank.sh`.

Using the following command, build queries for SystemDS.

```bash
# current directory: /evaluation/systemds/dense or /evaluation/systemds/sparse
bash build.sh
```

Once the build is successfully finished, run the following command on `dense` or `sparse` directory.

```bash
# current directory: /evaluation/systemds/dense or /evaluation/systemds/sparse
bash auto.sh
```

The experiment result will be shown at the end of each execution, looked similar to the following. 
Please record the value for the "Total elapsed time:" as its performance.

```
SystemDS Statistics:
Total elapsed time:             29.032 sec.     <=== HERE!!
Total compilation time:         0.355 sec.
Total execution time:           28.677 sec.
Number of compiled Spark inst:  3.
Number of executed Spark inst:  0.
Native openblas calls (dense mult/conv/bwdF/bwdD):      0/0/0/0.
Native openblas calls (sparse conv/bwdF/bwdD):  0/0/0.
Native openblas times (dense mult/conv/bwdF/bwdD):      0.000/0.000/0.000/0.000.
Cache hits (Mem/Li/WB/FS/HDFS): 33/0/0/0/3.
Cache writes (Li/WB/FS/HDFS):   2/12/0/1.
Cache times (ACQr/m, RLS, EXP): 23.319/0.000/0.002/0.036 sec.
HOP DAGs recompiled (PRED, SB): 0/0.
HOP DAGs recompile time:        0.000 sec.
```


### MLlib

Please make sure that you can run `spark-submit` anywhere and an environmental variable `SPARK_ROOT` is set appropriatly.
Also, please make sure that your Spark leverages OpenBLAS by running the below command in `spark-shell`.
Please refer to [the Spark documentation](https://archive.apache.org/dist/spark/docs/3.3.2/ml-linalg-guide.html) for more information.

```scala
import dev.ludovic.netlib.NativeBLAS
NativeBLAS.getInstance()
```

Before running, you may need to modify data paths in the source code.
Please move to the `mllib` directory.
Then, open `./src/main/scala/spark_ml_algs.scala` and modify paths for your environment.

Run the experiment script as below. 
Be aware that the temp directory (`spark.local.dir`) is set to the same storage as the data files for precise results.

```bash
# current directory: /evaluation/mllib/
bash ./auto.sh
```


### MADlib

#### Memory Setting

Please update configurations in `postgres.conf` file as follow to align with our evlaution:
```
shared_buffers = 12400MB
max_worker_processes = 1
max_parallel_workers_per_gather = 1
max_parallel_workers = 1
```

We ask reviewers to update `max_worker_processes`, `max_parallel_workers_per_gather`, and `max_parallel_workers` to 
the number of threads manually when parallelism experiments are required.


#### Data Loading

First, please move to the `madlib` directory.
Then, run the following commands to prepare for importing data to PostgreSQL.

```bash
# Current directory: /evaluation/madlib
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

We need to import data to PostgreSQL first.
Before importing, please open `./import-script/auto.sh` and `./sparse/import.sh` and check `DATAPATH` if it directs the right path.
Second, open `main.py` and update connection information (at line 6) to your environment.
After that, run the following commands to import data to PostgreSQL.

```bash
# Current directory: /evaluation/madlib
bash ./import-script/auto.sh
psql -f ./import-script/index.sql
bash ./sparse/import.sh
```

#### Experiment

To run dense experiments, move to the `exp-scripts` directory and run the following script.
Note that the script restarts the PostgreSQL service for each experiment.
The command could be different from our environment, thus please update the command if you need.

```bash
# Current directory: /evaluation/madlib/exp-scripts
bash auto.sh
```

To run sparse experiments, move to the `sparse` directory and run the following script.
Note that the script also contains restarting the PostgreSQL service.

```bash
# Current directory: /evaluation/madlib/sparse
bash auto.sh
```

### PreVision

If you have not built PreVision yet, please move to the root directory of this repository and run `makeall.sh`.
Make sure OpenBLAS, LAPACK, and LAPACKe are installed.

To run PreVision, go to the `prevision` directory.
If the `exec_eval` executable file not exist, build the executable file by running the `make` command.

To run dense and sparse experiments, run the following script.

```bash
# Current directory: /evaluation/prevision
bash ./exp.sh
```

Each experiment will report elapsed times and I/O volume such as below.

```
total   bf      io_r    io_ir   io_w    phit    preq    flgen   flget   sl      delhint pureplan
66374505        26980246        0       26381339        3670    2505    3915    23548   78738   6334    186     2863
0       0       0       14480002360     800     115440749616    158721027976    0       0       0       0       0
matmul  trans   elem    elem_c  elem_m  newarr
15860668        14385764        5215174 4       2877299 0
600     300     303     3       300     0
```

How to record:
- (The first line under header) The value under `total` is an elapsed time (microseconds). The "`total` - the other times below" should be recorded as CPU time in Fig 14.
- (The first line under header) The sum of `io_r`, `io_ir` and `io_w` is I/O time (microseconds), the sum of `flgen` and `pureplan` is query planning time (microseconds), and the sum of `flget` and `sl` is list maintenance time (microseconds). Those will be used for reproducing Fig 14 and Fig 15 in the paper.
- (The second line under header) The sum of `io_r` and `io_ir` is the I/O volume for read and the `io_w` is the I/O volume for write. Both read and write volumes are in bytes. Those will be used to reproduce Fig 11 in the paper.

To reproduce the blocking version of PreVision (for Fig 11), 
please move to `/lam_executor/src/` of this repository and replace `exec_interface.c` and `simulate.c` with `exec_interface_blocking.c` and `simulate_blocking.c`, respectively.
Then, build PreVision with `makeall.sh` in the root direcotry of this repository.
To run experiments, run `exp_blocking.sh` in the `/evaluation/prevision` directory.

PreVision is used to be used as a part of another system and these systems use the shared-memory for sharing buffers.
Thus, if you don't have enough shared memory space, you can run into a shared-memory error.
If so, please resize your `/dev/shm` and retry it.
To see how to increase the shared memory size, refer to [here](https://stackoverflow.com/a/58804023).

If you see `[BufferTile] shm_open failed. You can ignore this if you intend it.` error, please remove `/dev/shm/buffertile_*` files and retry it.
