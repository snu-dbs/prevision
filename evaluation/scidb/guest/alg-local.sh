#!/bin/bash
TIMEFORMAT='%3R'

source alg.sh

bash setup.sh

for i in $(seq 1 8)
do
    echo $i

    # Dense 
    lr 10M 3             # LR
    nmf 10M 3            # NMF

    lr 20M 3
    nmf 20M 3

    lr 40M 3            
    nmf 40M 3           

    lr 80M 3
    nmf 80M 3

    # Sparse
    sparse_lr 0_0125 3    # LR
    sparse_lr 0_025 3   
    sparse_lr 0_05 3   
    sparse_lr 0_1 3    

    # PageRank
    pagerank enron 3
    pagerank epinions 3
    pagerank livejournal 3
    pagerank twitter 3

    # iteration
    iterarray=(1 2 4 8 16 32)
    for noi in ${iterarray[@]}
    do
        echo $noi
        pagerank twitter $noi
    done
done

bash clean.sh