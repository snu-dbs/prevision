# Data Loading and Evaluation

sudo commands, so extend expireation time.

Source codes or scripts that we used for evaluation are organized as follow.
For detailed instructions, please refer to the below sections.

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

Please make sure you can call `spark-submit` in anywhere.
Also, sbt 1.8.2 should be installed on your computer.

Note that the storage device containing the current directory could be affect the performance.

To install another prerequisites that are needed to this experiment, run the following commands.

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

## Evaluation

### NumPy

The script for NumPy is located at `/evaluation/numpy_memmap/exp.sh`.
Please make sure the NumPy version is 1.22.4.
If you called `pip install -r requirements.txt`, then NumPy would be installed appropriately.
The `exp.sh` will runs all experiments presented in the paper.
Note that the script will copy some npy files to the current directory and run with these data.
The usage is as follow.

```bash
# current directory: /evaluation/numpy_memmap/
# make sure that we are on venv
bash exp.sh
```

If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp.sh` script to correctly direct the dataset path.

### Dask

The Dask evaluation script is located at `/evalaution/dask/exp_alg.sh`.
Please make sure that you have installed Dask 2022.6.0.
If you have executed `pip install -r requirements.txt`, then Dask 2022.6.0 would be installed appropriately.
The `exp_alg.sh` will runs all experiments presented in the paper.
The usage is as follow.

```bash
# current directory: /evaluation/dask/
# make sure that we are on venv
bash exp_alg.sh
```

If your directory structure is different from what we presented, please modify the `DATADIR` variable of the `exp_alg.sh` script to correctly direct the dataset path.

### SciDB

Before explaining SciDB stuffs, it is worth to mention that SciDB could not run when it has only one instance. 
Because of this reason, you may need to configure SciDB to use two instances.
It is also worth to say that sometimes it might causes an out-of-memory error because SciDB overuses memory capacity.
In such case, lowering memory numbers might make the workload run without OOM error.
Lastly, if you run SciDB using the root account, SciDB would make an MPI error. 
You can use a normal user to solve the issue.

The `/evaluation/scidb/guest/` contains SciDB queries, data loaders, and configuration file what we used.
The `load-dense.sh`, `load-sparse.sh`, and `load-pagerank.sh` are data-loading scripts.
You may need to modify the `DIR` variable of each scripts to adjust dataset directory.
The `alg.sh` script defines the matrix computation queries.
The `config.ini` file is a configuration we used for experiments.
The `setup.sh` and `clean.sh` are scripts that needed to be executed before and after experiments, respectively.

Becore running experiments, please run the data loading scripts.

If you run SciDB without docker, you can start evaluation with the following command.

```bash
# Note that alg-local.sh and alg.sh should be placed in the same directory.
# Note also that the setup.sh and clean.sh will be executed in the alg-local.sh script.
bash alg-local.sh
```

In case using docker, transfer the files in the `guest` directory to the docker container and use `/evaluation/scidb/exp.sh` on the host.
The commands in the `exp.sh` will be send to the docker container and execute queries remotely.
Your docker configuration may be different from ours, so please modify the `exp` function of the `exp.sh` for your environment to use it.

You are also required to call the `setup.sh` and `clean.sh` scripts inside the docker container.

You can start evaluation with the following command.

```bash
# Please configure exp.sh and put guest scripts appropriate directories.
bash exp.sh
```

### SystemDS

Please make sure the followings.
- You can run `spark-submit` in anywhere (i.e., add the spark `bin` directory to your `PATH` environmental variable). It is require to use SystemDS in out-of-core situation.
- You have set the `SYSTEMDS_ROOT` environmental variable.
- You have made SystemDS configuration on `$SYSTEMDS_ROOT/conf/`.


Before running an experiment, please move to `/evaluation/systemds/dense` or `/evaluation/systemds/sparse` directory depending on the experiment.
Then, open the `/src/main/scala/systemds_ml_algorithms.scala` file.
You can find `ml.setConfig()` statement near line 27 (for both dense and sparse cases).
Please make sure that the conf file name described in the line is same with yours.
If it is not the same, please update it.
After that, go back to the `dense` or `sparse` directory depending on the experiment.

Using the following command, build queries for SystemDS.

```bash
bash build.sh
```

Once build successfully finished, run a query like the follow commands.

```bash
# Dense
bash lr.sh [rows] [iteration] [input X] [input Y] [input W] [output]
bash nmf.sh [rows] [iteration] [input X] [input W] [input H] [output W] [output H]

# Sparse
bash lr.sh [rows] [iteration] [input X] [input Y] [input W] [output]

# PageRank
bash pagerank.sh [rows] [iteration] [input] [output]
```

Following snippet is an example of running SystemDS experiment.

```bash
# PageRank, Enron, iteration=3
bash pagerank.sh 36692 3 ../../../slab-benchmark/prevision/output/sysds/enron output

# PageRank, Enron, iteration=3
bash pagerank.sh 61578415 32 ../../../slab-benchmark/prevision/output/sysds/twitter output
```

### MLlib

Please make sure that you can run `spark-submit` in anywhere (i.e., add the spark `bin` directory to your `PATH` environmental variable).

Before running, you may need to modify the paths of MLlib data files in the MLlib source code.
Please open `./src/main/scala/spark_ml_algs.scala` and modify paths for your environment.
Then, run the experiment script as follow.

```bash
bash ./auto.sh
```

### MADlib

First, please move to the `./madlib/` directory.
Then, run the following commands to prepare for importing data to PostgreSQL.

```bash
# Current directory: madlib
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

We need to import data to PostgreSQL first.
Before importing, please open `./import-script/auto.sh` and `./sparse/import.sh` and modify `DATAPATH` and `DATADIR` values, respectively.
After editing, run the following commands to import data to PostgreSQL.

```bash
# Current directory: madlib
bash ./import-script/auto.sh
psql -f ./import-script/index.sql
bash ./sparse/import.sh
```

To run dense experiments, run the following script.
Note that the script restarts the postgresql serivce for each experiment.
The service name could be different from our environment, thus please update the service name if you need.

```bash
bash ./exp-scripts/auto.sh
```

To run sparse experiments, run the following script.
Note that the script also contains restarting postgresql service.
Please also note that MADlib could raise an dimension mismatch error when running sparse experiments.
This is because MADlib infers matrix sizes using inserted cell values.
To solve this issue, just insert that a left-upper cell with zero value (i.e., `row_id=1, col_id=1, value=0`) and a right-lower cell with zero value.

```bash
bash ./sparse/auto.sh
```

### PreVision

To run PreVision, go to the `prevision` directory.
If you have generated matrices for PreVision, `exec_eval` executable file would exist.
If not, build the executable file by run `make` command.

To run dense and sparse experiments, run the following script.

```bash
bash ./exp.sh
```

If you run into the shared-memory error, please resize your `/dev/shm` and retry it.
To see how to increase the shared memory size, refer to [here](https://stackoverflow.com/a/58804023).

If you see `[BufferTile] shm_open failed. You can ignore this if you intend it.` error, please remove `/dev/shm/buffertile_*` files and retry it.