#!/bin/bash
TIMEFORMAT='%3R'

source /home/scidb/alg.sh
$1 $2
exit;

sparse_lr 0_0125
sparse_lr 0_025

lr 80M
lr 40M
lr 20M
lr 10M

nmf 80M
nmf 40M
nmf 20M
nmf 10M
