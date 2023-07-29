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

function pr() {
	iter=$1
	xname=$2
	vname=$3
	N=$4
	noi=$5
	echo "PageRank"
	
	for j in $(seq 1 $iter); 
	do 
		echo "iter=$j"
		# setup
		sudo service postgresql@12-main restart;
		sleep 10;
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

		# algorithm
		time psql -f pr_setup.sql -v v1=$xname -v v2=$vname -v v3=$N
		time psql -f pr1.sql -v v1=$xname -v v2=$vname -v v3=$N
		for i in $(seq 1 $noi); 
		do 
			time psql -f pr2.sql -v v1=$xname -v v2=$vname -v v3=$N
		done;
		time psql -f pr_end.sql -v v1=$xname -v v2=$vname -v v3=$N
	done;
}

lr 8 mat_400mx100_sparse_0_0125 mat_400mx1_sparse_0_0125 mat_100x1_sparse_0_0125 3
lr 8 mat_400mx100_sparse_0_025 mat_400mx1_sparse_0_025 mat_100x1_sparse_0_025 3 
lr 8 mat_400mx100_sparse_0_05 mat_400mx1_sparse_0_05 mat_100x1_sparse_0_05 3
lr 8 mat_400mx100_sparse_0_1 mat_400mx1_sparse_0_1 mat_100x1_sparse_0_1 3

pr 8 mat_enron mat_enron_v 36692 3
pr 8 mat_epinions mat_epinions_v 75888 3
pr 8 mat_livejournal mat_livejournal_v 4847571 3
pr 8 mat_twitter mat_twitter_v 61578415 3

iterarray=(1 2 4 8 16 32)
for noi in ${iterarray[@]}
do
	echo $noi
	pr 8 mat_twitter mat_twitter_v 61578415 $noi
done
