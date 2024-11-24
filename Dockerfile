FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC 

##############################
# Install Dependencies
#############################

# Directory
RUN mkdir /data

# Install essential
RUN apt-get update && apt-get install -y \
	build-essential \
	curl \
	python3.8-dev python3-pip \
	git \
	time \
	gawk \
	wget

# Install OpenBLAS
RUN cd /data && \
	git clone --depth 1 -b v0.3.0 https://github.com/OpenMathLib/OpenBLAS.git && \
	cd OpenBLAS && \
	make -j9 && \
	make install

# Install Docker
RUN cd /data && curl -fsSL https://get.docker.com -o get-docker.sh && \
	sh get-docker.sh

# Install Java
RUN apt-get install -y openjdk-11-jdk

# Install Postgres Dependencies
RUN apt install -y python2.7-dev cmake sudo libreadline-dev zlib1g-dev flex bison

##############################
# Clone Repository & setup
#############################
# RUN git clone --depth 1 -b docker https://github.com/snu-dbs/prevision.git /data/prevision
# COPY . /data/prevision

# Install SBT
RUN cd /data && wget https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz && \
	tar zxvf sbt-1.8.2.tgz
ENV PATH="$PATH:/data/sbt/bin"

##############################
# Install Systems
#############################
# Install Spark
RUN cd /data && \
	curl -O https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
	tar zxvf spark-3.3.2-bin-hadoop3.tgz
ENV PATH="$PATH:/data/spark-3.3.2-bin-hadoop3/bin"

# Install SystemDS
RUN cd /data && \
	curl -O https://archive.apache.org/dist/systemds/3.1.0/systemds-3.1.0-bin.tgz && \
	tar zxvf systemds-3.1.0-bin.tgz
ENV PATH="$PATH:/data/systemds-3.1.0-bin/bin"
ENV SYSTEMDS_ROOT=/data/systemds-3.1.0-bin

# SciDB: SciDB docker image will be pulled when starting a container since docker command cannot be called here
RUN sed -i '62s/^/#/' /etc/init.d/docker

# Install MADlib
# Postgres First
RUN cd /data && curl -O https://ftp.postgresql.org/pub/source/v12.14/postgresql-12.14.tar.gz && \
	tar zxvf postgresql-12.14.tar.gz && cd postgresql-12.14 && \
	./configure --with-python && make -j9 && make install
RUN adduser postgres && \
	mkdir /usr/local/pgsql/data && \
	chown postgres /usr/local/pgsql/data
RUN sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data && \
	sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l /usr/local/pgsql/data/logfile start && \
	sudo -u postgres /usr/local/pgsql/bin/createuser root && \
	sudo -u postgres /usr/local/pgsql/bin/createdb -O root root && \
	sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data stop
ENV PATH="$PATH:/usr/local/pgsql/bin"

# MADlib
RUN cd /data && curl -O https://dist.apache.org/repos/dist/release/madlib/1.21.0/apache-madlib-1.21.0-src.tar.gz && \
	tar zxvf apache-madlib-1.21.0-src.tar.gz && \
	cd apache-madlib-1.21.0-src && mkdir build && cd build && \
	cmake .. && make
RUN sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l /usr/local/pgsql/data/logfile start && \
	sudo chmod a+x /data/apache-madlib-1.21.0-src/src/bin/madpack && \
	sudo -E -u postgres sh -c "PATH=$PATH:/usr/local/pgsql/bin/data/" apache-madlib-1.21.0-src/src/bin/madpack -s madlib -p postgres install && \
	sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data stop

##############################
# Setup and install package
##############################

COPY . /data/prevision

# Build MLlib src
ENV SPARK_ROOT="/data/spark-3.3.2-bin-hadoop3"
RUN cd /data/prevision/evaluation/mllib && \
	sbt clean && \
	sbt assembly

# Build SystemDS src
RUN cd /data/prevision/evaluation/systemds/dense && \
	bash build.sh && \
	cd ../sparse && \
	bash build.sh

# SystemDS config file
COPY evaluation/systemds/SystemDS-config.xml /data/systemds-3.1.0-bin/conf/

# Postgres 
# config file
COPY evaluation/madlib/postgresql.conf /usr/local/pgsql/data/postgresql.conf

# import tool
RUN cd /data && wget https://bootstrap.pypa.io/pip/2.7/get-pip.py && \
        python2.7 get-pip.py && \
	apt-get install -y libpq-dev
RUN cd /data/prevision/evaluation/madlib && \
	pip2.7 install -r requirements.txt

# Install NumPy and Dask
RUN cd /data/prevision/evaluation && \
	pip3 install --upgrade pip && \
	pip3 install -r requirements.txt

# Build PreVision
ENV LD_LIBRARY_PATH=/opt/OpenBLAS/lib
RUN apt-get install -y liblapacke-dev liblapack-dev
RUN cd /data/prevision && bash makeall.sh

