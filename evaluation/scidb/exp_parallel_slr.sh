DOCKER_NAME="prevision-scidb-exp"
SCRIPT_PATH="/prevision/evaluation/scidb/guest/alg-remote.sh"
CONFIG="config_p2.ini"

function init() {
	sudo docker start $DOCKER_NAME
	sleep 16

	echo "Database Initialization"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py stop"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "cp /prevision/evaluation/scidb/guest/config/""$CONFIG"" /opt/scidb/19.11/etc/config.ini"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "echo 'y' | /opt/scidb/19.11/bin/scidbctl.py init-cluster"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "/opt/scidb/19.11/bin/scidbctl.py start"

	echo "Dataset Load"
	sudo docker exec -it $DOCKER_NAME sudo -u scidb bash -c "PATH=/opt/scidb/19.11/bin:$PATH /prevision/evaluation/scidb/guest/load-sparse-slr00125.sh"
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
		sleep 16
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sudo docker exec -it $DOCKER_NAME bash $SCRIPT_PATH $1 $2 $noi
		sudo docker stop $DOCKER_NAME
	done
}

# Evaluation
init

echo "###############################################"
echo "# parallelism: 2"
echo "###############################################"
exp sparse_lr 0_0125 3   