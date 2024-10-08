**For reviewers, we opened an FTP server to provide access to the entire dataset used by the repository. Please [email us](mailto:koo@dbs.snu.ac.kr).**

# Data Generator for PreVision

Before getting started, we would like to say thanks for reproducing this work!
Since the paper involves seven systems with huge volumes of datasets, you might need at least several terabytes of free disk space and are required to have several dozen hours to generate datasets.
If you want to generate data by yourself, please read this documentation.

This documentation describes how to generate matrices used in the paper.
Before running the below, please make sure that the following prerequisites are already prepared.

## Prerequisites

Since some systems throw an out-of-memory error when trying to generate datasets, we recommend using a machine with a large memory size.

Please make sure that the below are installed.
We recommend using tested versions to avoid issues.

- Docker: 24.0.2 tested
- Python 2: 2.7.17 and 2.7.18 tested
- Virtualenv: 16.7.7 and 20.0.17 tested
- Python 3: 3.8.2 and 3.8.17 tested
- Python 3 venv
- Spark 3.3.2
- SystemDS 3.1.0
- Java 11
- GNU parallel
- OpenBLAS
- LAPACK and LAPACKe

## Directory Structure

The directory structure of the data generator looks like follow. 
We are currently in the `prevision` directory.
We will run commands here.

```
slab-benchmark
├── analysis
├── config
├── data
├── gen_data.py -> ./lib/python/gen_data.py
├── lib
├── prevision               # PreVision Data Generator
│   ├── README.md           # README
│   ├── generator           # Generation Scripts
│   ├── mtd                 # Some metadata
│   ├── output              # Data Output Directory
│   ├── requirements2.txt
│   ├── requirements3.txt
│   └── tmp                 # Tmp Directory for Some Systems
├── requirements.txt
├── sql_cxn.py -> ./lib/python/sql_cxn.py
└── tests
```

<!-- ## One-shot Script

We provide a one-shot script to generate all matrices used in the paper.
If you run into some issues, please go through the next section, and manually generate matrices.
Since this script will generate all matrices for each system, you should make sure that you have enough disk space.

```bash
# Make sure that the current directory is '/prevision/'
sudo bash auto.sh
``` -->

## Matrix Generation

To reproduce the paper, we need to create dense tall-skinny matrices, sparse tall-skinny matrices, and square matrices for PageRank.
This generator relies on the [SLAB benchmark](https://adalabucsd.github.io/slab.html) for generating tall-skinny matrices.
This repository is based on the benchmark and we already made some modifications, so you don't need to download it again.

First, we are going to generate CSV files using the SLAB benchmark.
Using the generated CSV files and PageRank CSV files that were downloaded from the Internet (we will describe PageRank stuff soon), we will convert those to each format of the comparison systems.

### Generating CSV Files for Tall-Skinny Matrices

#### Preparation

Since SLAB uses Python 2, create a virtualenv environment using Python 2 and install requirements as follows.

```bash
virtualenv -p $(which python2) venv2
source ./venv2/bin/activate
pip install -r requirements2.txt
deactivate
```

Some parts of our generator use Python 3. 
Therefore, please create venv for Python 3 and install the requirements as follows.

```bash
python3 -m venv venv3
source ./venv3/bin/activate
pip install -r requirements3.txt
deactivate
```

The followings are the issues we have run into and the solution we used. 

- `RuntimeError: failed to query python2.7 with code 1 err: ... SyntaxError: invalid syntax\` when virtualenv: virtualenv has dropped supports for Python 2.x so you need to install an older version (e.g., 20.0.17)
- `pg_ctl command not found`: It might be raised because you don't have a development environment for PostgreSQL. You need to install `libpq-dev` on your computer.
- `#include <Python.h>: No such file or directory`: It might be raised because you don't have a development environment for Python. You need to install `python3-dev` and `python2-dev` on your computer.

#### Generation

We need to use PostgreSQL because the SLAB benchmark generates CSV files using PostgreSQL.
Using the PostgreSQL that we already configured could ruin your environment (i.e., MADlib), so we are going to do it using Docker.
Using the following commands, please pull a Postgres image and create a Postgres container.
If you are already using the 15432 port, use another one and modify `/slab-benchmark/lib/python/gen_data.py` to use your port number.

```bash
sudo docker pull postgres
sudo docker run -p15432:5432 --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

Now, you can generate matrices with the following command.
The output matrices will be generated in the `./output/csv/` directory.

```bash
sudo bash ./generator/auto_gen_csv.sh

# a parallel version; if an error occurs, please use the above
sudo bash ./generator/auto_gen_csv_parallel.sh
```

After generating CSV files, please make sure that you have the appropriate permission for all generated CSV files.
The following command will ensure the file owner and permission.
Please check if the environmental variable `USER` is set appropriately. 

```bash
sudo chown -R $USER ./output/*
sudo chmod -R 755 ./output/*
```

Finally, we need to put the finishing touches on the sparse matrices.
Since the density of an SLAB-generated matrix is not accurate, we generated matrices with higher densities.
Thus, the density of matrices should be lowered.
The following command performs that.

```bash
bash ./generator/auto_lower_density.sh

# a parallel version; if an error occurs, please use the above
bash ./generator/auto_lower_density_parallel.sh
```

### Generating CSV Files for PageRank Matrices

The paper uses four graph datasets for PageRank experiments, namely Enron, Epinions, LiveJournal, and Twitter.
First of all, please download all files to the `./output/raw/` directory.
The links and filenames of each dataset are as follows:
- [Enron](https://snap.stanford.edu/data/email-Enron.html): `email-Enron.txt`
- [Epinions](https://snap.stanford.edu/data/soc-Epinions1.html): `soc-Epinions1.txt`
- [LiveJournal](https://snap.stanford.edu/data/soc-LiveJournal1.html): `soc-LiveJournal1.txt`
- [Twitter](https://github.com/ANLAB-KAIST/traces/releases/tag/twitter_rv.net): `twitter_rv.net`

After downloading, please remove the comments at the top of each file and run the following command to generate CSV files for PageRank experiments.
This command will produce the CSV files in the `./output/csv/` directory.

```bash
bash ./generator/auto_conv_raw_to_csv.sh
```

### Conversion for each system

#### Converting CSV to NumPy data format

Since NumPy only supports dense computation, we only need to convert dense matrices.
The following command will make npy files for dense experiments.
The outputs will be generated in the `./output/npy/` directory.

```bash
bash ./generator/auto_conv_csv_to_npy.sh

# OR, you can use the parallel version of the script
bash ./generator/auto_conv_csv_to_npy_parallel.sh
```

#### Converting CSV to Dask HDF5 format

Dask only supports dense computation, so we only need to convert dense matrices.
Dask is the second-best performer in the dense NMF with the 80M matrix, so we create matrices with finer tile sizes as well.
The following command will make HDF files from npy files for dense experiments.
The matrices will be generated in the `./output/hdf5/` directory.

```bash
bash ./generator/auto_conv_npy_to_hdf5.sh

# OR, you can use the parallel version of the script
bash ./generator/auto_conv_npy_to_hdf5_parallel.sh
```

#### Converting CSV to SystemDS binary format

SystemDS requires a JSON-formatted metadata file (MTD) for each data file.
The MTD files for PageRank should be supplied, so please copy these files using the following command.

```bash
cp ./mtd/* ./output/csv/
```

Before converting, please make sure that you can call the `systemds` command anywhere (i.e., the command path should be included in `PATH`).
Please also make sure that the environmental variable `SYSTEMDS_ROOT` is set appropriately.

The following command automatically generates binary files for both dense and sparse experiments.
The results will be saved in `./output/sysds/`.

```bash
bash ./generator/auto_conv_csv_to_bin.sh
```

You can control the memory size that is used during the conversion.
In line 86 of `$SYSTEMDS_ROOT/bin/systemds` (version 3.1.0), you can find the `SYSTEMDS_STANDALONE_OPTS` parameter.
You can increase the maximum Java heap memory size by changing `-Xmx4g`.

#### Converting for SciDB

SciDB can load CSV files in the COO format.
However, the dense matrices that SLAB generated are not in the COO format, so we need to convert them to the COO format.
The following command automatically converts it and saves the results in `./output/scidb/`.

```bash
bash ./generator/auto_conv_densecsv_to_coocsv.sh
```

#### Converting for MLlib

Conversion for MLlib relies on `spark-shell`, thus please make sure that you can call the command anywhere (i.e., the command path is included in `PATH`).

You can configure memory size and the number of executors using the `./generator/auto_conv_csv_to_sf.sh` file.
The `MEM_CONF` specifies the memory size, and the `MASTER` specifies the master URL of Spark.
Since Spark could use more memory than what we specified, please configure the memory parameter carefully.

The following command will produce matrices for MLlib to `./output/sequencefile/`.

```bash
bash ./generator/auto_conv_csv_to_sf.sh
```

#### Converting CSV to PreVision data format

The following command will produce `.tilestore` data files that PreVision uses as its matrix format. 
Please run the following command.
The result files will be placed on `./output/prevision/`.

```bash
bash ./generator/auto_conv_csv_to_prevision.sh
```

PreVision is used to be used as a part of another system and these systems use the shared-memory for sharing buffers.
Thus, if you don't have enough shared memory space, you can run into a shared-memory error.
If so, please resize your `/dev/shm` and retry it.
To see how to increase the shared memory size, refer to [here](https://stackoverflow.com/a/58804023).

If you see `[BufferTile] shm_open failed. You can ignore this if you intend it.` error, please remove `/dev/shm/buffertile_*` files and retry it.


Congratulations! 
You're done!
Now, you can run experiments using generated matrices.
Please move on to the `/evaluation/README.md`.
