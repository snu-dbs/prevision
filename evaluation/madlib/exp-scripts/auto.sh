function nmf() {
	iter=$1
	xname=$2
	wname=$3
	hname=$4
	noi=$5
	
	echo "NMF"
	for j in $(seq 1 $iter); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		# algorithm
		time psql -f nmf_setup.sql -v v1=$xname -v v2=$wname -v v3=$hname
		for i in $(seq 1 $noi); 
		do 
			time psql -f nmf.sql -v v1=$xname -v v2=$wname -v v3=$hname
		done;
		time psql -f nmf_end.sql -v v1=$xname -v v2=$wname -v v3=$hname
	done;
}

function lr() {
	iter=$1
	xname=$2
	yname=$3
	wname=$4
	noi=$5
	echo "LR"
	
	for j in $(seq 1 $iter); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		# algorithm
		time psql -f lr_setup.sql -v v1=$xname -v v2=$yname -v v3=$wname
		time psql -f lr1.sql -v v1=$xname -v v2=$yname -v v3=$wname
		for i in $(seq 1 $noi); 
		do 
			time psql -f lr2.sql -v v1=$xname -v v2=$yname -v v3=$wname
		done;
		time psql -f lr_end.sql -v v1=$xname -v v2=$yname -v v3=$wname
	done;
}

# NMF & LR

nmf 1 mat_10mx100_dense mat_10mx10_dense mat_10x100_dense 3
nmf 1 mat_20mx100_dense mat_20mx10_dense mat_10x100_dense 3
nmf 1 mat_40mx100_dense mat_40mx10_dense mat_10x100_dense 3
nmf 1 mat_80mx100_dense mat_80mx10_dense mat_10x100_dense 3

lr 1 mat_10mx100_dense mat_10mx1_dense vec_100x1_dense 3
lr 1 mat_20mx100_dense mat_20mx1_dense vec_100x1_dense 3
lr 1 mat_40mx100_dense mat_40mx1_dense vec_100x1_dense 3
lr 1 mat_80mx100_dense mat_80mx1_dense vec_100x1_dense 3

# Iteration 
iterarray=(1 2 4 8 16 32)
for noi in ${iterarray[@]}
do
    echo $noi
	nmf 1 mat_10mx100_dense mat_10mx10_dense mat_10x100_dense $noi
done

#NOTE: Please do parallelism experiments manually