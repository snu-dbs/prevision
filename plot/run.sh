#!/bin/bash
sudo docker run -it \
    --privileged \
	-v $(pwd)/..:/data/prevision \
	grammaright/prevision-plot:latest sh -c "cd /data/prevision/plot; python3 gen_all.py"
