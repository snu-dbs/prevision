#!/bin/bash
DATAPATH=/mnt/hdd/prevision-ari/output

###########################
# NumPy
###########################
# LR
bash evaluate.sh numpy lr 10m 3 1 1 $DATAPATH
bash evaluate.sh numpy lr 20m 3 1 1 $DATAPATH
bash evaluate.sh numpy lr 40m 3 1 1 $DATAPATH
bash evaluate.sh numpy lr 80m 3 1 1 $DATAPATH

# NMF
bash evaluate.sh numpy nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh numpy nmf 20m 3 1 1 $DATAPATH
bash evaluate.sh numpy nmf 40m 3 1 1 $DATAPATH
bash evaluate.sh numpy nmf 80m 3 1 1 $DATAPATH

# iteration
bash evaluate.sh numpy nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 32 1 1 $DATAPATH

# parallelism
bash evaluate.sh numpy nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh numpy nmf 10m 3 8 1 $DATAPATH

###########################
# Dask
###########################
# LR (fig 8)
bash evaluate.sh dask lr 10m 3 1 1 $DATAPATH
bash evaluate.sh dask lr 20m 3 1 1 $DATAPATH
bash evaluate.sh dask lr 40m 3 1 1 $DATAPATH
bash evaluate.sh dask lr 80m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh dask nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 20m 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 40m 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 80m 3 1 1 $DATAPATH

# iteration (fig 9)
bash evaluate.sh dask nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh dask nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh dask nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh dask nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh dask nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh dask nmf 10m 32 1 1 $DATAPATH

# parallelism (fig 10)
bash evaluate.sh dask nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh dask nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh dask nmf 10m 3 8 1 $DATAPATH

# smalltiles (fig 15)
bash evaluate.sh dask nmf 80m_200x1 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 80m_400x1 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 80m_800x1 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 80m_1600x1 3 1 1 $DATAPATH
bash evaluate.sh dask nmf 80m_3200x1 3 1 1 $DATAPATH

###########################
# MADlib
###########################
# LR (fig 8)
bash evaluate.sh madlib lr 10m 3 1 1 $DATAPATH
bash evaluate.sh madlib lr 20m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh madlib nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh madlib nmf 20m 3 1 1 $DATAPATH

# SLR (fig 8)
bash evaluate.sh madlib slr 0.0125 3 1 1 $DATAPATH

# PageRank (fig 8)
bash evaluate.sh madlib pagerank enron 3 1 1 $DATAPATH
bash evaluate.sh madlib pagerank epinions 3 1 1 $DATAPATH
bash evaluate.sh madlib pagerank livejournal 3 1 1 $DATAPATH
bash evaluate.sh madlib pagerank twitter 3 1 1 $DATAPATH

# iteration - nmf (fig 9)
bash evaluate.sh madlib nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 32 1 1 $DATAPATH

# iteration - pagerank (fig 9)
bash evaluate.sh madlib pagerank twitter 1 1 1 $DATAPATH
bash evaluate.sh madlib pagerank twitter 2 1 1 $DATAPATH
bash evaluate.sh madlib pagerank twitter 4 1 1 $DATAPATH
bash evaluate.sh madlib pagerank twitter 8 1 1 $DATAPATH

# parallelism - nmf (fig 10)
bash evaluate.sh madlib nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh madlib nmf 10m 3 8 1 $DATAPATH

# parallelism - slr (fig 10)
bash evaluate.sh madlib slr 0.0125 3 2 1 $DATAPATH
bash evaluate.sh madlib slr 0.0125 3 4 1 $DATAPATH
bash evaluate.sh madlib slr 0.0125 3 8 1 $DATAPATH

###########################
# SystemDS
###########################
# LR (fig 8)
bash evaluate.sh systemds lr 10m 3 1 1 $DATAPATH
bash evaluate.sh systemds lr 20m 3 1 1 $DATAPATH
bash evaluate.sh systemds lr 40m 3 1 1 $DATAPATH
bash evaluate.sh systemds lr 80m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh systemds nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh systemds nmf 20m 3 1 1 $DATAPATH
bash evaluate.sh systemds nmf 40m 3 1 1 $DATAPATH
bash evaluate.sh systemds nmf 80m 3 1 1 $DATAPATH

# SLR (fig 8)
bash evaluate.sh systemds slr 0.0125 3 1 1 $DATAPATH
bash evaluate.sh systemds slr 0.025 3 1 1 $DATAPATH
bash evaluate.sh systemds slr 0.05 3 1 1 $DATAPATH
bash evaluate.sh systemds slr 0.1 3 1 1 $DATAPATH

# PageRank (fig 8)
bash evaluate.sh systemds pagerank enron 3 1 1 $DATAPATH
bash evaluate.sh systemds pagerank epinions 3 1 1 $DATAPATH
bash evaluate.sh systemds pagerank livejournal 3 1 1 $DATAPATH

# iteration - nmf (fig 9)
bash evaluate.sh systemds nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 32 1 1 $DATAPATH

# parallelism - nmf (fig 10)
bash evaluate.sh systemds nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh systemds nmf 10m 3 8 1 $DATAPATH

# parallelism - slr (fig 10)
bash evaluate.sh systemds slr 0.0125 3 2 1 $DATAPATH
bash evaluate.sh systemds slr 0.0125 3 4 1 $DATAPATH
bash evaluate.sh systemds slr 0.0125 3 8 1 $DATAPATH

###########################
# SciDB
###########################
# LR (fig 8)
bash evaluate.sh scidb lr 10m 3 1 1 $DATAPATH
bash evaluate.sh scidb lr 20m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh scidb nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh scidb nmf 20m 3 1 1 $DATAPATH

# SLR (fig 8)
bash evaluate.sh scidb slr 0.0125 3 1 1 $DATAPATH
bash evaluate.sh scidb slr 0.025 3 1 1 $DATAPATH

# PageRank (fig 8)
bash evaluate.sh scidb pagerank enron 3 1 1 $DATAPATH
bash evaluate.sh scidb pagerank epinions 3 1 1 $DATAPATH
bash evaluate.sh scidb pagerank livejournal 3 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 3 1 1 $DATAPATH

# iteration - nmf (fig 9)
bash evaluate.sh scidb nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 32 1 1 $DATAPATH

# iteration - pagerank (fig 9)
bash evaluate.sh scidb pagerank twitter 1 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 2 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 4 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 8 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 16 1 1 $DATAPATH
bash evaluate.sh scidb pagerank twitter 32 1 1 $DATAPATH

# parallelism - nmf (fig 10)
bash evaluate.sh scidb nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh scidb nmf 10m 3 8 1 $DATAPATH

# parallelism - slr (fig 10)
bash evaluate.sh scidb slr 0.0125 3 2 1 $DATAPATH

###########################
# MLlib
###########################
# LR (fig 8)
bash evaluate.sh mllib lr 10m 3 1 1 $DATAPATH
bash evaluate.sh mllib lr 20m 3 1 1 $DATAPATH
bash evaluate.sh mllib lr 40m 3 1 1 $DATAPATH
bash evaluate.sh mllib lr 80m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh mllib nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh mllib nmf 20m 3 1 1 $DATAPATH
bash evaluate.sh mllib nmf 40m 3 1 1 $DATAPATH
bash evaluate.sh mllib nmf 80m 3 1 1 $DATAPATH

# SLR (fig 8)
bash evaluate.sh mllib slr 0.0125 3 1 1 $DATAPATH
bash evaluate.sh mllib slr 0.025 3 1 1 $DATAPATH
bash evaluate.sh mllib slr 0.05 3 1 1 $DATAPATH
bash evaluate.sh mllib slr 0.1 3 1 1 $DATAPATH

# PageRank (fig 8)
bash evaluate.sh mllib pagerank enron 3 1 1 $DATAPATH
bash evaluate.sh mllib pagerank epinions 3 1 1 $DATAPATH
bash evaluate.sh mllib pagerank livejournal 3 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 3 1 1 $DATAPATH

# iteration - nmf (fig 9)
bash evaluate.sh mllib nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 32 1 1 $DATAPATH

# iteration - pagerank (fig 9)
bash evaluate.sh mllib pagerank twitter 1 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 2 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 4 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 8 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 16 1 1 $DATAPATH
bash evaluate.sh mllib pagerank twitter 32 1 1 $DATAPATH

# parallelism - nmf (fig 10)
bash evaluate.sh mllib nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh mllib nmf 10m 3 8 1 $DATAPATH

# parallelism - slr (fig 10)
bash evaluate.sh mllib slr 0.0125 3 2 1 $DATAPATH
bash evaluate.sh mllib slr 0.0125 3 4 1 $DATAPATH
bash evaluate.sh mllib slr 0.0125 3 8 1 $DATAPATH

###########################
# PreVision
###########################
# LR (fig 8)
bash evaluate.sh prevision lr 10m 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 20m 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 40m 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 80m 3 1 1 $DATAPATH

# NMF (fig 8)
bash evaluate.sh prevision nmf 10m 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 20m 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 40m 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 80m 3 1 1 $DATAPATH

# SLR (fig 8)
bash evaluate.sh prevision slr 0.0125 3 1 1 $DATAPATH
bash evaluate.sh prevision slr 0.025 3 1 1 $DATAPATH
bash evaluate.sh prevision slr 0.05 3 1 1 $DATAPATH
bash evaluate.sh prevision slr 0.1 3 1 1 $DATAPATH

# PageRank (fig 8)
bash evaluate.sh prevision pagerank enron 3 1 1 $DATAPATH
bash evaluate.sh prevision pagerank epinions 3 1 1 $DATAPATH
bash evaluate.sh prevision pagerank livejournal 3 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 3 1 1 $DATAPATH

# iteration - nmf (fig 9)
bash evaluate.sh prevision nmf 10m 1 1 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 2 1 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 4 1 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 8 1 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 16 1 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 32 1 1 $DATAPATH

# iteration - pagerank (fig 9)
bash evaluate.sh prevision pagerank twitter 1 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 2 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 4 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 8 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 16 1 1 $DATAPATH
bash evaluate.sh prevision pagerank twitter 32 1 1 $DATAPATH

# parallelism - nmf (fig 10)
bash evaluate.sh prevision nmf 10m 3 2 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 3 4 1 $DATAPATH
bash evaluate.sh prevision nmf 10m 3 8 1 $DATAPATH

# parallelism - slr (fig 10)
bash evaluate.sh prevision slr 0.0125 3 2 1 $DATAPATH
bash evaluate.sh prevision slr 0.0125 3 4 1 $DATAPATH
bash evaluate.sh prevision slr 0.0125 3 8 1 $DATAPATH

# I/O (figure 11)
bash evaluate.sh prevision_wo_pe lr 80m 3 1 1 $DATAPATH
bash evaluate.sh prevision_wo_pe 80m 3 1 1 $DATAPATH

bash evaluate.sh prevision_blocking lr 80m 3 1 1 $DATAPATH
bash evaluate.sh prevision_blocking 80m 3 1 1 $DATAPATH

bash evaluate.sh prevision_blocking_wo_pe lr 80m 3 1 1 $DATAPATH
bash evaluate.sh prevision_blocking_wo_pe 80m 3 1 1 $DATAPATH

# Buffer Replacement (figure 14)
bash evaluate.sh prevision_mru lr 80m 3 1 1 $DATAPATH
bash evaluate.sh prevision_mru 80m 3 1 1 $DATAPATH

bash evaluate.sh prevision_lruk lr 80m 3 1 1 $DATAPATH
bash evaluate.sh prevision_lruk 80m 3 1 1 $DATAPATH

# smalltiles - lr (fig 15)
bash evaluate.sh prevision lr 80m_200x1 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 80m_400x1 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 80m_800x1 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 80m_1600x1 3 1 1 $DATAPATH
bash evaluate.sh prevision lr 80m_3200x1 3 1 1 $DATAPATH

# smalltiles - nmf (fig 15)
bash evaluate.sh prevision nmf 80m_200x1 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 80m_400x1 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 80m_800x1 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 80m_1600x1 3 1 1 $DATAPATH
bash evaluate.sh prevision nmf 80m_3200x1 3 1 1 $DATAPATH
