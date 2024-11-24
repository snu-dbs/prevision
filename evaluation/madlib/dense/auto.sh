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
		/usr/bin/time -f '%e' psql -f lr_setup.sql -v v1=$xname -v v2=$yname -v v3=$wname  2>&1 | tee -a /tmp/exp_result.log
		/usr/bin/time -f '%e' psql -f lr1.sql -v v1=$xname -v v2=$yname -v v3=$wname  2>&1 | tee -a /tmp/exp_result.log
		for i in $(seq 1 $noi); 
		do 
			/usr/bin/time -f '%e' psql -f lr2.sql -v v1=$xname -v v2=$yname -v v3=$wname  2>&1 | tee -a /tmp/exp_result.log
		done;
		/usr/bin/time -f '%e' psql -f lr_end.sql -v v1=$xname -v v2=$yname -v v3=$wname  2>&1 | tee -a /tmp/exp_result.log
	done;
}

function nmf() {
	xname=$1
	wname=$2
	hname=$3
	
	echo "NMF"
	for j in $(seq 1 $repetition); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

		# algorithm
		/usr/bin/time -f '%e' psql -f nmf_setup.sql -v v1=$xname -v v2=$wname -v v3=$hname 2>&1 | tee -a /tmp/exp_result.log
		for i in $(seq 1 $noi); 
		do 
			/usr/bin/time -f '%e' psql -f nmf.sql -v v1=$xname -v v2=$wname -v v3=$hname 2>&1 | tee -a /tmp/exp_result.log
		done;
		/usr/bin/time -f '%e' psql -f nmf_end.sql -v v1=$xname -v v2=$wname -v v3=$hname 2>&1 | tee -a /tmp/exp_result.log
	done;
}

    
# set parallelism
echo "max_worker_processes = "$p >> /usr/local/pgsql/data/postgresql.conf
echo "max_parallel_workers_per_gather = "$p >> /usr/local/pgsql/data/postgresql.conf
echo "max_parallel_workers = "$p >> /usr/local/pgsql/data/postgresql.conf

# run task
if [[ $task == "nmf" ]]; then
	if [[ $data == "10m" ]]; then
		nmf mat_10mx100_dense mat_10mx10_dense mat_10x100_dense
	elif [[ $data == "20m" ]]; then
		nmf mat_20mx100_dense mat_20mx10_dense mat_10x100_dense
	elif [[ $data == "40m" ]]; then
		nmf mat_40mx100_dense mat_40mx10_dense mat_10x100_dense
	elif [[ $data == "80m" ]]; then
		nmf mat_80mx100_dense mat_80mx10_dense mat_10x100_dense
	fi
elif [[ $task == "lr" ]]; then
	if [[ $data == "10m" ]]; then
		lr mat_10mx100_dense mat_10mx1_dense vec_100x1_dense
	elif [[ $data == "20m" ]]; then
		lr mat_20mx100_dense mat_20mx1_dense vec_100x1_dense
	elif [[ $data == "40m" ]]; then
		lr mat_40mx100_dense mat_40mx1_dense vec_100x1_dense
	elif [[ $data == "80m" ]]; then
		lr mat_80mx100_dense mat_80mx1_dense vec_100x1_dense
	fi
fi


# collect result
awk -F "," '{if (NF == 1 && $1 ~ /^[0-9]*\.?[0-9]+$/) {sum += $1}} END {print sum}' /tmp/exp_result.log >> "/data/results/time-madlib-"$task"-"$data"-"$noi"-"$p".log" 
