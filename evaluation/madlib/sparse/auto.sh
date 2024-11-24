#!/bin/bash

# input arguments
task=$1
data=$2
noi=$3
p=$4
repetition=$5

function lr() {
	xname=$1
	yname=$2
	wname=$3
	echo "LR"
	
	for j in $(seq 1 $repetition); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

		# algorithm
		/usr/bin/time -f '%e' psql -f lr_setup.sql -v v1=$xname -v v2=$yname -v v3=$wname 2>&1 | tee -a /tmp/exp_result.log
		/usr/bin/time -f '%e' psql -f lr1.sql -v v1=$xname -v v2=$yname -v v3=$wname 2>&1 | tee -a /tmp/exp_result.log
		for i in $(seq 1 $noi); 
		do 
			/usr/bin/time -f '%e' psql -f lr2.sql -v v1=$xname -v v2=$yname -v v3=$wname 2>&1 | tee -a /tmp/exp_result.log
		done;
		/usr/bin/time -f '%e' psql -f lr_end.sql -v v1=$xname -v v2=$yname -v v3=$wname 2>&1 | tee -a /tmp/exp_result.log
	done;
}

function pr() {
	xname=$1
	vname=$2
	N=$3
	echo "PageRank"
	
	for j in $(seq 1 $repetition); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

		# algorithm
		/usr/bin/time -f '%e' psql -f pr_setup.sql -v v1=$xname -v v2=$vname -v v3=$N 2>&1 | tee -a /tmp/exp_result.log
		/usr/bin/time -f '%e' psql -f pr1.sql -v v1=$xname -v v2=$vname -v v3=$N 2>&1 | tee -a /tmp/exp_result.log
		for i in $(seq 1 $noi); 
		do 
			/usr/bin/time -f '%e' psql -f pr2.sql -v v1=$xname -v v2=$vname -v v3=$N 2>&1 | tee -a /tmp/exp_result.log
		done;
		/usr/bin/time -f '%e' psql -f pr_end.sql -v v1=$xname -v v2=$vname -v v3=$N 2>&1 | tee -a /tmp/exp_result.log
	done;
}

# set parallelism
echo "max_worker_processes = "$p >> /usr/local/pgsql/data/postgresql.conf
echo "max_parallel_workers_per_gather = "$p >> /usr/local/pgsql/data/postgresql.conf
echo "max_parallel_workers = "$p >> /usr/local/pgsql/data/postgresql.conf

# run task
if [[ $task == "slr" ]]; then
	if [[ $data == "0.0125" ]]; then
		lr mat_400mx100_sparse_0_0125 mat_400mx1_sparse_0_0125 mat_100x1_sparse_0_0125
	elif [[ $data == "0.025" ]]; then
		lr mat_400mx100_sparse_0_025 mat_400mx1_sparse_0_025 mat_100x1_sparse_0_025 
	elif [[ $data == "0.05" ]]; then
		lr mat_400mx100_sparse_0_05 mat_400mx1_sparse_0_05 mat_100x1_sparse_0_05
	elif [[ $data == "0.1" ]]; then
		lr mat_400mx100_sparse_0_1 mat_400mx1_sparse_0_1 mat_100x1_sparse_0_1
	fi
elif [[ $task == "pagerank" ]]; then
	if [[ $data == "enron" ]]; then
		pr mat_enron mat_enron_v 36692
	elif [[ $data == "epinions" ]]; then
		pr mat_epinions mat_epinions_v 75888
	elif [[ $data == "livejournal" ]]; then
		pr mat_livejournal mat_livejournal_v 4847571
	elif [[ $data == "twitter" ]]; then
		pr mat_twitter mat_twitter_v 61578415
	fi
fi

# collect result
awk -F "," '{if (NF == 1 && $1 ~ /^[0-9]*\.?[0-9]+$/) {sum += $1}} END {print sum}' /tmp/exp_result.log >> "/data/results/time-madlib-"$task"-"$data"-"$noi"-"$p".log" 
