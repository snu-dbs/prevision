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
The recorded elapsed times will be used to compare with the results.

### NumPy

The script for NumPy is located at `./numpy_memmap/exp.sh`.
The `exp.sh` will run all experiments presented in the paper.
Note that the script will copy some npy files to the current directory and run with these data.
The usage is as follows.

```bash
# current directory: /evaluation/numpy_memmap/
# make sure that we are on venv
bash exp.sh
```

If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp.sh` script to correctly direct the dataset path.

### Dask

The Dask evaluation script is located at `./dask/exp_alg.sh`.
The `exp_alg.sh` will run all experiments presented in the paper.
The usage is as follows.

```bash
# current directory: /evaluation/dask/
# make sure that we are on venv
bash exp_alg.sh
```

If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp_alg.sh` script to correctly direct the dataset path.

### SciDB

Before explaining SciDB stuff, it is worth mentioning that SciDB could not run when it has only one instance. 
Because of this reason, you may need to configure SciDB to use two instances.
It is also worth saying that sometimes it might cause an out-of-memory error because SciDB overuses memory capacity.
In such a case, lowering memory numbers might make the workload run without an OOM error.
Lastly, if you run SciDB using the root account, SciDB would make an MPI error. 
You can use a normal user to solve the issue.

The `./scidb/guest/` contains SciDB queries, data loaders, and configuration files that we used.
The `load-dense.sh`, `load-sparse.sh`, and `load-pagerank.sh` are data-loading scripts.
You may need to modify the `DIR` variable of each script to adjust the dataset directory.
The `alg.sh` script defines the matrix computation queries.
The `config.ini` file is a configuration we used for experiments.
The `setup.sh` and `clean.sh` are scripts that needed to be executed before and after experiments, respectively.

Before running experiments, please run the data loading scripts.

If you run SciDB without docker, you can start the evaluation with the following command.
Please note that you may need to drop the OS page cache after every experiment.

```bash
# Current directory: /evaluation/scidb/guest/
# Note that alg-local.sh and alg.sh should be placed in the same directory.
# Note also that the setup.sh and clean.sh will be executed in the alg-local.sh script.
bash alg-local.sh
```

In case using docker, transfer the files in the `guest` directory to the docker container and use `./scidb/exp.sh` on the host.
The commands in the `exp.sh` will be sent to the docker container and execute queries remotely.
Your docker configuration may be different from ours, so please modify the `exp.sh` and `alg-remote.sh` for your environment to use these.

You are also required to call the `setup.sh` and `clean.sh` scripts inside the docker container.

You can start an evaluation with the following command.

```bash
# Current directory: /evaluation/scidb/
# Please configure exp.sh and alg-remote.sh and put guest scripts in appropriate directories.
bash exp.sh
```

### SystemDS

Please make sure the following.
- You can run `spark-submit` anywhere (i.e., add the spark `bin` directory to your `PATH` environmental variable). It is required to use SystemDS in out-of-core situations.
- You have set the `SYSTEMDS_ROOT` environmental variable.
- You have made SystemDS configuration on `$SYSTEMDS_ROOT/conf/`.


Before running an experiment, please move to `./systemds/dense` or `./systemds/sparse` directory depending on the experiment.
Then, open the `./src/main/scala/systemds_ml_algorithms.scala` file.
You can find `ml.setConfig()` statement near line 27 (for both dense and sparse cases).
Please make sure that the config file name described in the line is the same as yours.
If it is not the same, please update it.
After that, go back to the `dense` or `sparse` directory depending on the experiment.

Please note that the LR task uses OpenBLAS, whereas the NMF task does not. You can find the `ml.setConfigProperty()` statements in lines 54 and 57 in the `./src/main/scala/systemds_ml_algorithms.scala` file. This configuration is necessary because SystemDS produces incorrect results during the NMF task when multiplying a matrix by its transpose (specifically, computing $HH^T$).

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


Since SystemDS relies on Spark for out-of-core computation, its performance is highly dependent on the configuration of Spark.
Below is an excerpt from our paper regarding the configuration used:

> For SystemDS and MLlib, Spark was configured to run with a driver process and a single executor process.
> We did not isolate JVM processes to a specific core in order to prevent performance degradation caused by garbage collection or just-in-time compilation.
> We tested a few memory configurations to discover a specific memory budget for each driver and executor that delivers the best performance.

Below is the specific memory allocation with a total budget of 27GB (only successful experiments are presented):

#### Dense LR
| Number of Rows | 10M  | 20M  | 40M  | 80M  |
|----------------|------|------|------|------|
| **Driver Memory**   | 26GB | 26GB | 7GB  | 14GB |
| **Executor Memory** | 1GB  | 1GB  | 20GB | 13GB |

---

#### Dense NMF
| Number of Rows | 10M  | 20M  | 40M  | 80M  |
|----------------|------|------|------|------|
| **Driver Memory**   | 26GB | 26GB | 20GB | 20GB |
| **Executor Memory** | 1GB  | 1GB  | 7GB  | 7GB  |

---

#### Sparse LR
| Density        | 0.0125 | 0.025 | 0.05  | 0.1   |
|----------------|--------|-------|-------|-------|
| **Driver Memory**   | 1GB    | 7GB   | 1GB   | 1GB   |
| **Executor Memory** | 26GB   | 20GB  | 26GB  | 26GB  |

---

#### PageRank
| Dataset Label  | Enron  | Epinions | Livejournal |
|----------------|--------|----------|-------------|
| **Driver Memory**   | 7GB    | 20GB     | 14GB        |
| **Executor Memory** | 20GB   | 7GB      | 13GB        |


### MLlib

Please make sure that you can run `spark-submit` anywhere (i.e., add the spark `bin` directory to your `PATH` environmental variable).

Before running, you may need to modify the paths of MLlib data files in the MLlib source code.
Please move to the `mllib` directory.
Then, open `./src/main/scala/spark_ml_algs.scala` and modify paths for your environment.
Then, run the experiment script as follow.

```bash
# current directory: /evaluation/systemds/mllib/
bash ./auto.sh
```

### MADlib

First, please move to the `madlib` directory.
Then, run the following commands to prepare for importing data to PostgreSQL.

```bash
# Current directory: madlib
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

We need to import data to PostgreSQL first.
Before importing, please open `./import-script/auto.sh` and `./sparse/import.sh` and check `DATAPATH` if it directs the right path.
After that, run the following commands to import data to PostgreSQL.

```bash
# Current directory: /evaluation/madlib
bash ./import-script/auto.sh
psql -f ./import-script/index.sql
bash ./sparse/import.sh
```

To run dense experiments, move to the `exp-scripts` directory and run the following script.
Note that the script restarts the PostgreSQL service for each experiment.
The command to restart could be different from our environment, thus please update the command if you need.

```bash
# Current directory: /evaluation/madlib/exp-scripts
bash auto.sh
```

To run sparse experiments, run the following script.
Note that the script also contains restarting the PostgreSQL service.
Please also note that MADlib could raise a dimension mismatch error when running sparse experiments.
This is because MADlib infers matrix sizes using inserted cell values.
To solve this issue, just insert an upper-left cell with zero value (i.e., `row_id=1, col_id=1, value=0`) and a lower-right cell with zero value.

```bash
# Current directory: madlib
bash ./sparse/auto.sh
```

### PreVision

To run PreVision, go to the `prevision` directory.
If you have generated matrices for PreVision, the `exec_eval` executable file would exist.
If not, build the executable file by running the `make` command.

To run dense and sparse experiments, run the following script.

```bash
# Current directory: prevision
bash ./exp.sh
```

PreVision is used to be used as a part of another system and these systems use the shared-memory for sharing buffers.
Thus, if you don't have enough shared memory space, you can run into a shared-memory error.
If so, please resize your `/dev/shm` and retry it.
To see how to increase the shared memory size, refer to [here](https://stackoverflow.com/a/58804023).

If you see `[BufferTile] shm_open failed. You can ignore this if you intend it.` error, please remove `/dev/shm/buffertile_*` files and retry it.
