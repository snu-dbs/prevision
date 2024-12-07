# PreVision: An Out-of-Core Matrix Computation System with Optimal Buffer Replacement

A Dockerized version of the PreVision repository.
If you find any errors or bugs, please [contact us](mailto:koo@dbs.snu.ac.kr).
If there is any problem in the repository, we will notify you.

**How to run experiments?:**
1. Make sure that you have downloaded pre-generated dataset from our FTP server.
2. Move to the `evaluation` directory.
3. Customize the `run.sh` script.
    - Update the `DATAPATH` variable to point the `output` directory that downloaded from our FTP server. Our Docker container will copy/import dataset from the directory.
    - (Optional) The entire experiments would take about a week. If you don't want to run the entire things, please delete commands you don't want to evaluate. The usage of the `evaluate.sh` script is `bash evaluate.sh [SYSTEM] [TASK] [DATA] [ITER] [PARALLELISM] [REPETITION] [DATAPATH]`.
4. Run experiments with `sudo bash run.sh 2>&1 | tee -a result.log`.

The results will be store in the `results` directory.

**How to generate graphs?:**
1. Move to the `plot` directory.
2. Please run `bash run.sh`.
3. Generated plots will be stored in the `output` directory.

**How to build Docker images?:**
- The scripts to evaluate and plot download Docker images from DockerHub. However, you can build Docker images on your machine.
  - Two Dockerfiles are in this repository.
    - `Dockerfile` in the root directory sets up systems and prepares for experiments.
    - `plot/Dockerfile` sets up an environment to generate plots in the paper.
- To use the Docker images, update `docker run` commands in `evaluation/evaluate.sh` and `plot/run.sh` to point your tags.

