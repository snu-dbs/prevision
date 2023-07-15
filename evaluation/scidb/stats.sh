while true;
do
	free | head -n 2 | tail -n 1 >> mem.log
	df -h | grep sda >> df.log
	sleep 1
done;
