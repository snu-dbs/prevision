#!/bin/bash
DATAPATH=/mnt/hdd/prevision-ari/output

fig=$1
dataset=$2

if [[ $fig == "fig8a" ]]; then
    if [[ $dataset == "10m" ]]; then
        #bash evaluate.sh numpy lr 10m 3 1 1 $DATAPATH
        #bash evaluate.sh dask lr 10m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision lr 10m 3 1 1 $DATAPATH
        bash evaluate.sh systemds lr 10m 3 1 1 $DATAPATH
        bash evaluate.sh mllib lr 10m 3 1 1 $DATAPATH
        bash evaluate.sh scidb lr 10m 3 1 1 $DATAPATH
        #bash evaluate.sh madlib lr 10m 3 1 1 $DATAPATH
    elif [[ $dataset == "20m" ]]; then
        #bash evaluate.sh numpy lr 20m 3 1 1 $DATAPATH
        #bash evaluate.sh dask lr 20m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision lr 20m 3 1 1 $DATAPATH
        bash evaluate.sh systemds lr 20m 3 1 1 $DATAPATH
        bash evaluate.sh mllib lr 20m 3 1 1 $DATAPATH
        # bash evaluate.sh scidb lr 20m 3 1 1 $DATAPATH
        #bash evaluate.sh madlib lr 20m 3 1 1 $DATAPATH
    elif [[ $dataset == "40m" ]]; then
        #bash evaluate.sh numpy lr 40m 3 1 1 $DATAPATH
        #bash evaluate.sh dask lr 40m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision lr 40m 3 1 1 $DATAPATH
        bash evaluate.sh systemds lr 40m 3 1 1 $DATAPATH
        bash evaluate.sh mllib lr 40m 3 1 1 $DATAPATH
    elif [[ $dataset == "80m" ]]; then
        #bash evaluate.sh numpy lr 80m 3 1 1 $DATAPATH
        #bash evaluate.sh dask lr 80m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision lr 80m 3 1 1 $DATAPATH
        bash evaluate.sh systemds lr 80m 3 1 1 $DATAPATH
        bash evaluate.sh mllib lr 80m 3 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig8b" ]]; then
    if [[ $dataset == "10m" ]]; then
        #bash evaluate.sh numpy nmf 10m 3 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 3 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 3 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 3 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 3 1 1 $DATAPATH
        #bash evaluate.sh madlib nmf 10m 3 1 1 $DATAPATH
    elif [[ $dataset == "20m" ]]; then
        #bash evaluate.sh numpy nmf 20m 3 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 20m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 20m 3 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 20m 3 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 20m 3 1 1 $DATAPATH
        # bash evaluate.sh scidb nmf 20m 3 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 20m 3 1 1 $DATAPATH
    elif [[ $dataset == "40m" ]]; then
        #bash evaluate.sh numpy nmf 40m 3 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 40m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 40m 3 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 40m 3 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 40m 3 1 1 $DATAPATH
    elif [[ $dataset == "80m" ]]; then
        #bash evaluate.sh numpy nmf 80m 3 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 80m 3 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 80m 3 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 80m 3 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 80m 3 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig8c" ]]; then
    if [[ $dataset == "0.0125" ]]; then
        #bash evaluate.sh prevision slr 0.0125 3 1 1 $DATAPATH
        bash evaluate.sh systemds slr 0.0125 3 1 1 $DATAPATH
        bash evaluate.sh mllib slr 0.0125 3 1 1 $DATAPATH
        bash evaluate.sh scidb slr 0.0125 3 1 1 $DATAPATH
        #bash evaluate.sh madlib slr 0.0125 3 1 1 $DATAPATH
    elif [[ $dataset == "0.025" ]]; then
        #bash evaluate.sh prevision slr 0.025 3 1 1 $DATAPATH
        bash evaluate.sh systemds slr 0.025 3 1 1 $DATAPATH
        bash evaluate.sh mllib slr 0.025 3 1 1 $DATAPATH
        bash evaluate.sh scidb slr 0.025 3 1 1 $DATAPATH
    elif [[ $dataset == "0.05" ]]; then
        #bash evaluate.sh prevision slr 0.05 3 1 1 $DATAPATH
        bash evaluate.sh systemds slr 0.05 3 1 1 $DATAPATH
        bash evaluate.sh mllib slr 0.05 3 1 1 $DATAPATH
    elif [[ $dataset == "0.1" ]]; then
        #bash evaluate.sh prevision slr 0.1 3 1 1 $DATAPATH
        bash evaluate.sh systemds slr 0.1 3 1 1 $DATAPATH
        bash evaluate.sh mllib slr 0.1 3 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig8d" ]]; then
    if [[ $dataset == "enron" ]]; then
        #bash evaluate.sh prevision pagerank enron 3 1 1 $DATAPATH
        bash evaluate.sh systemds pagerank enron 3 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank enron 3 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank enron 3 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank enron 3 1 1 $DATAPATH
    elif [[ $dataset == "epinions" ]]; then
        #bash evaluate.sh prevision pagerank epinions 3 1 1 $DATAPATH
        bash evaluate.sh systemds pagerank epinions 3 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank epinions 3 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank epinions 3 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank epinions 3 1 1 $DATAPATH
    elif [[ $dataset == "livejournal" ]]; then
        #bash evaluate.sh prevision pagerank livejournal 3 1 1 $DATAPATH
        bash evaluate.sh systemds pagerank livejournal 3 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank livejournal 3 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank livejournal 3 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank livejournal 3 1 1 $DATAPATH
    elif [[ $dataset == "twitter" ]]; then
        #bash evaluate.sh prevision pagerank twitter 3 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 3 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 3 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank twitter 3 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig9a" ]]; then
    if [[ $dataset == "1" ]]; then
        #bash evaluate.sh numpy nmf 10m 1 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 1 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 1 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 1 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 1 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 1 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 1 1 1 $DATAPATH
    elif [[ $dataset == "2" ]]; then
        #bash evaluate.sh numpy nmf 10m 2 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 2 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 2 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 2 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 2 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 2 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 2 1 1 $DATAPATH
    elif [[ $dataset == "4" ]]; then
        #bash evaluate.sh numpy nmf 10m 4 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 4 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 4 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 4 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 4 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 4 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 4 1 1 $DATAPATH
    elif [[ $dataset == "8" ]]; then
        #bash evaluate.sh numpy nmf 10m 8 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 8 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 8 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 8 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 8 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 8 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 8 1 1 $DATAPATH
    elif [[ $dataset == "16" ]]; then
        #bash evaluate.sh numpy nmf 10m 16 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 16 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 16 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 16 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 16 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 16 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 16 1 1 $DATAPATH
    elif [[ $dataset == "32" ]]; then
        #bash evaluate.sh numpy nmf 10m 32 1 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 32 1 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 32 1 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 32 1 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 32 1 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 32 1 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 32 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig9b" ]]; then
    if [[ $dataset == "1" ]]; then
        #bash evaluate.sh prevision pagerank twitter 1 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 1 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 1 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank twitter 1 1 1 $DATAPATH
    elif [[ $dataset == "2" ]]; then
        #bash evaluate.sh prevision pagerank twitter 2 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 2 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 2 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank twitter 2 1 1 $DATAPATH
    elif [[ $dataset == "4" ]]; then
        #bash evaluate.sh prevision pagerank twitter 4 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 4 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 4 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank twitter 4 1 1 $DATAPATH
    elif [[ $dataset == "8" ]]; then
        #bash evaluate.sh prevision pagerank twitter 8 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 8 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 8 1 1 $DATAPATH
        bash evaluate.sh madlib pagerank twitter 8 1 1 $DATAPATH
    elif [[ $dataset == "16" ]]; then
        #bash evaluate.sh prevision pagerank twitter 16 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 16 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 16 1 1 $DATAPATH
    elif [[ $dataset == "32" ]]; then
        #bash evaluate.sh prevision pagerank twitter 32 1 1 $DATAPATH
        bash evaluate.sh mllib pagerank twitter 32 1 1 $DATAPATH
        bash evaluate.sh scidb pagerank twitter 32 1 1 $DATAPATH
    fi
elif [[ $fig  == "fig10a" ]]; then
    if [[ $dataset == "2" ]]; then
        #bash evaluate.sh numpy nmf 10m 3 2 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 3 2 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 3 2 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 3 2 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 3 2 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 3 2 1 $DATAPATH
        #bash evaluate.sh madlib nmf 10m 3 2 1 $DATAPATH
    elif [[ $dataset == "4" ]]; then
        #bash evaluate.sh numpy nmf 10m 3 4 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 3 4 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 3 4 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 3 4 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 3 4 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 3 4 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 3 4 1 $DATAPATH
    elif [[ $dataset == "8" ]]; then
        #bash evaluate.sh numpy nmf 10m 3 8 1 $DATAPATH
        #bash evaluate.sh dask nmf 10m 3 8 1 $DATAPATH
        #bash evaluate.sh prevision nmf 10m 3 8 1 $DATAPATH
        bash evaluate.sh systemds nmf 10m 3 8 1 $DATAPATH
        bash evaluate.sh mllib nmf 10m 3 8 1 $DATAPATH
        bash evaluate.sh scidb nmf 10m 3 8 1 $DATAPATH
        bash evaluate.sh madlib nmf 10m 3 8 1 $DATAPATH
    fi
elif [[ $fig  == "fig10b" ]]; then
    if [[ $dataset == "2" ]]; then
        #bash evaluate.sh prevision slr 0.0125 3 2 1 $DATAPATH
        bash evaluate.sh systemds slr 0.0125 3 2 1 $DATAPATH
        bash evaluate.sh mllib slr 0.0125 3 2 1 $DATAPATH
        bash evaluate.sh scidb slr 0.0125 3 2 1 $DATAPATH
        bash evaluate.sh madlib slr 0.0125 3 2 1 $DATAPATH
    elif [[ $dataset == "4" ]]; then
        #bash evaluate.sh prevision slr 0.0125 3 4 1 $DATAPATH
        bash evaluate.sh systemds slr 0.0125 3 4 1 $DATAPATH
        bash evaluate.sh mllib slr 0.0125 3 4 1 $DATAPATH
        bash evaluate.sh madlib slr 0.0125 3 4 1 $DATAPATH
    elif [[ $dataset == "8" ]]; then
        #bash evaluate.sh prevision slr 0.0125 3 8 1 $DATAPATH
        bash evaluate.sh systemds slr 0.0125 3 8 1 $DATAPATH
        bash evaluate.sh mllib slr 0.0125 3 8 1 $DATAPATH
        bash evaluate.sh madlib slr 0.0125 3 8 1 $DATAPATH
    fi
fi

