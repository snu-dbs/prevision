function exp() {
	iter=8
	noi=$3

	echo $1 $2 $3

	for i in $(seq 1 $iter)
	do
		echo "iter="$i
		sudo docker start prevision-scidb-exp
		sleep 16
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sudo docker exec -it prevision-scidb-exp bash /home/scidb/alg-remote.sh $1 $2 $noi
		sudo docker stop prevision-scidb-exp
	done
}

# Evaluation

# Dense 
exp lr 10M 3             # LR
exp nmf 10M 3            # NMF

exp lr 20M 3
exp nmf 20M 3

exp lr 40M 3            
exp nmf 40M 3           

exp lr 80M 3
exp nmf 80M 3

# Sparse
exp sparse_lr 0_0125 3    # LR
exp sparse_lr 0_025 3   
exp sparse_lr 0_05 3   
exp sparse_lr 0_1 3    

# PageRank
exp pagerank enron 3
exp pagerank epinions 3
exp pagerank livejournal 3
exp pagerank twitter 3

# iteration
iterarray=(1 2 4 8 16 32)
for noi in ${iterarray[@]}
do
    echo $noi
    exp nmf 10M $noi
    exp pagerank twitter $noi
done