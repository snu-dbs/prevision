#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5

DOCKER_NAME="prevision-scidb-exp"
SCRIPT_PATH="/data/prevision/evaluation/scidb/guest/alg-remote.sh"
SCIDB_RESULT_PATH="/data/scidb_result"

function init_normal() {
	CONFIG="config.ini"

	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker restart $DOCKER_NAME
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-dense-all.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function init_sparse_7500() {
	CONFIG="config.ini"

	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker restart $DOCKER_NAME
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-sparse-slr00125.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-pagerank-small.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function init_sparse_4000() {
	CONFIG="config_4000.ini"

	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker restart $DOCKER_NAME
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-sparse-slr0025.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-pagerank-twitter.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function init_parallel_nmf() {
	CONFIG=$1
	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker restart $DOCKER_NAME
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-dense-10m.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function init_parallel_slr() {
	CONFIG="config_p2.ini"
	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker restart $DOCKER_NAME
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-sparse-slr00125.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function exp() {
	noi=$3

	for i in $(seq 1 $repetition)
	do
        echo "###############################################"
        echo "# Algorithm: $1"
        echo "# Dataset: $2"
        echo "# Iterations: $noi"
        echo "###############################################"

		sudo docker start $DOCKER_NAME
		sleep 10
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sudo docker exec -it $DOCKER_NAME bash $SCRIPT_PATH $1 $2 $noi
		sudo docker stop $DOCKER_NAME
	done
}

# create a docker container; first volume for repository and second volume for scidb result
service docker start
docker run --name $DOCKER_NAME -dit --shm-size=30gb -v /data/prevision:/prevision -v $SCIDB_RESULT_PATH:/data grammaright/scidb:19.11-xenial

# run task
if [[ $task == "lr" ]]; then
	init
	if [[ $data == "10m" ]]; then
		exp lr 10M $iter
	elif [[ $data == "20m" ]]; then
		exp lr 20M $iter
	elif [[ $data == "40m" ]]; then
		exp lr 40M $iter
	elif [[ $data == "80m" ]]; then
		exp lr 80M $iter
	fi
elif [[ $task == "nmf" ]]; then
	if [[ $data == "10m" ]]; then
		init
		exp nmf 10M $iter
	elif [[ $data == "20m" ]]; then
		init
		exp nmf 20M $iter
	elif [[ $data == "40m" ]]; then
		init
		exp nmf 40M $iter
	elif [[ $data == "80m" ]]; then
		if [[ $p == "1" ]]; then
			init
		elif [[ $p == "2" ]]; then
			init_parallel_nmf "config_p2.ini"
		elif [[ $p == "4" ]]; then
			init_parallel_nmf "config_p4.ini"
		elif [[ $p == "8" ]]; then
			init_parallel_nmf "config_p8.ini"
		fi
		exp nmf 80M $iter
	fi
elif [[ $task == "slr" ]]; then
	if [[ $data == "0.0125" ]]; then
		if [[ $p == "1" ]]; then
			init_sparse_7500
		elif [[ $p == "2" ]]; then
			init_parallel_slr
		fi
		exp sparse_lr 0_0125 $iter
	elif [[ $data == "0.025" ]]; then
		init_sparse_4000
		exp sparse_lr 0_025 $iter
	elif [[ $data == "0.05" ]]; then
		init_sparse_4000
		exp sparse_lr 0_05 $iter
	elif [[ $data == "0.1" ]]; then
		init_sparse_4000
		exp sparse_lr 0_1 $iter
	fi
elif [[ $task == "pagerank" ]]; then
	if [[ $data == "enron" ]]; then
		init_sparse_7500
		exp pagerank enron $iter
	elif [[ $data == "epinions" ]]; then
		init_sparse_7500
		exp pagerank epinions $iter
	elif [[ $data == "livejournal" ]]; then
		init_sparse_7500
		exp pagerank livejournal $iter
	elif [[ $data == "twitter" ]]; then
		init_sparse_4000
		exp pagerank twitter $iter
	fi
fi

# collect result
awk -F "," '{if (NF == 1 && $1 ~ /^[0-9]*\.?[0-9]+$/) {sum += $1}} END {print sum}' $SCIDB_RESULT_PATH"/exp_result.log" >> "/data/prevision/evaluation/results/time-scidb-"$task"-"$data"-"$iter"-"$p".log" 
