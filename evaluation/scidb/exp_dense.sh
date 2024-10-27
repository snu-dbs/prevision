DOCKER_NAME="prevision-scidb-exp"
SCRIPT_PATH="/prevision/evaluation/scidb/guest/alg-remote.sh"
CONFIG="config.ini"

function init() {
	sudo docker start $DOCKER_NAME
	sleep 10

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME bash -c "chown scidb /dbpath; chmod a+x /prevision/evaluation/scidb/guest/*"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py start"
	sleep 10

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-dense-all.sh"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/setup.sh"

	echo "Done"
	sudo docker stop $DOCKER_NAME
}

function exp() {
	iter=1
	noi=$3

	for i in $(seq 1 $iter)
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

# Evaluation
init

################
# Dense LR
################
exp lr 10M 3             
exp lr 20M 3
# OOM
# exp lr 40M 3            
# exp lr 80M 3

################
# Dense NMF
################
exp nmf 10M 3            
exp nmf 20M 3
# OOM
# exp nmf 40M 3           
# exp nmf 80M 3

# iteration
iterarray=(1 2 4 8 16 32)
for noi in ${iterarray[@]}
do
	echo "###############################################"
	echo "# num of iteration: $noi"
	echo "###############################################"
    exp nmf 10M $noi
done