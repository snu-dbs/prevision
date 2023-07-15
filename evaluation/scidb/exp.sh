function exp() {
	iter=7
	noi=$3

	echo $1 $2

	for i in $(seq 1 $iter)
	do
		echo "iter="$i
		sudo docker start tilepack-scidb-exp
		sleep 16
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sudo docker exec -it tilepack-scidb-exp bash /home/scidb/alg-remote.sh $1 $2 $noi
		sudo docker stop tilepack-scidb-exp
	done
}


iterarray=(32)
for noi in ${iterarray[@]}
do
        echo $noi
	# exp nmf 10M $noi
	#exp pagerank enron $noi
	#exp pagerank epinions $noi
	#exp pagerank livejournal $noi
	exp pagerank twitter $noi
done
exit;


# exp sparse_lr 0_0125
exp sparse_lr 0_025

exit;
exp lr 10M
exp nmf 10M

exp lr 20M
exp nmf 20M

# exp lr 40M
# exp nmf 40M
