#!/bin/bash
TIMEFORMAT='%3R'

function lr() {
	DATASET=$1
	iter=$2

	IN_X='mat_'$DATASET'x100_dense'
	IN_y='mat_'$DATASET'x1_dense'
	IN_w='mat_100x1_dense'

	##############################
	# LR
	##############################
	echo "LR "$DATASET

	# setup
	iquery -aq 'remove(w)'

	# rename
	iquery -aq 'rename('$IN_X', X)'
	iquery -aq 'rename('$IN_y', y)'
	iquery -aq 'rename('$IN_w', w)'

	iquery -aq 'rename(zero_'$DATASET'x1, zero_1)'
	iquery -aq 'rename(zero_100x1, zero_2)'

	# do
	for i in $(seq 1 $iter);
	do
		# iter
		time iquery -aq 'store(
		    project(
		    apply(join(w, project(
		    apply(
		    gemm(X,
		    project(apply(
			join(gemm(X, w, zero_1), y), delta, pow(1+EXP(-gemm), -1) - value), 
		    delta), zero_2, transa:true), norm, 0.0000001*gemm), norm)
		), update, value - norm), update), w_new)'

		# change name
		if [ $i -eq 1 ]
		then
			time iquery -aq 'rename(w, '$IN_w')'
		else
			time iquery -aq 'remove(w)'
		fi

		time iquery -aq 'store(project(apply(w_new, value, update), value), w)'
		time iquery -aq 'remove(w_new)'
	done

	# restore
	iquery -aq 'rename(X, '$IN_X')'
	iquery -aq 'rename(y, '$IN_y')'

	iquery -aq 'rename(zero_1, zero_'$DATASET'x1)'
	iquery -aq 'rename(zero_2, zero_100x1)'
}

function nmf() {
	DATASET=$1
	iter=$2

	IN_X='mat_'$DATASET'x100_dense'
	IN_W='mat_'$DATASET'x10_dense'
	IN_H='mat_10x100_dense'

	##############################
	# NMF
	##############################
	echo "NMF "$DATASET

	# setup
	iquery -aq 'remove(W_old)'
	iquery -aq 'remove(H_old)'

	# rename
	iquery -aq 'rename('$IN_X', X)'
	iquery -aq 'rename('$IN_W', W_old)'
	iquery -aq 'rename('$IN_H', H_old)'

	iquery -aq 'rename(ZXHT_'$DATASET', ZXHT)'
	iquery -aq 'rename(ZWHHT_'$DATASET', ZWHHT)'

	# do
	for i in $(seq 1 $iter);
	do
		# build W
		time iquery -aq 'store(project(apply(project(apply(join(W_old,
		    project(apply(join(
			project(apply(gemm(X, H_old, ZXHT, transb: true), prod, gemm), prod),
			gemm(W_old, gemm(H_old, H_old, zero_10x10, transb: true), ZWHHT)),
		    div, prod/gemm), div)),
		value_new, value*div), value_new), value, value_new), value), W_new)'

		if [ $i -eq 1 ]
		then
			time iquery -aq 'rename(W_old, '$IN_W')'
		else
			time iquery -aq 'remove(W_old)'
		fi
		time iquery -aq 'rename(W_new, W_old)'

		# build H
		time iquery -aq 'store(project(apply(project(apply(join(H_old,
		    project(apply(join(
			project(apply(gemm(W_old, X, zero_10x100, transa: true), prod, gemm), prod),
			gemm(gemm(W_old, W_old, zero_10x10, transa: true), H_old, zero_10x100)),
		    div, prod/gemm), div)),
		value_new, value*div), value_new), value, value_new), value), H_new)'

		# change name
		if [ $i -eq 1 ]
		then
			time iquery -aq 'rename(H_old, '$IN_H')'
		else
			time iquery -aq 'remove(H_old)'
		fi
		time iquery -aq 'rename(H_new, H_old)'
	done

	# restore
	iquery -aq 'rename(X, '$IN_X')'

	iquery -aq 'rename(ZXHT, ZXHT_'$DATASET')'
	iquery -aq 'rename(ZWHHT, ZWHHT_'$DATASET')'
}

function sparse_lr() {
	DATASET=$1
	iter=$2

	IN_X='mat_400Mx100_sparse_'$DATASET
	IN_y='mat_400Mx1_sparse_'$DATASET
	IN_w='mat_100x1_sparse_'$DATASET

	##############################
	# LR
	##############################
	echo "SPARSE LR "$DATASET

	# setup
	iquery -aq 'remove(w)'

	# rename
	iquery -aq 'rename('$IN_X', X)'
	iquery -aq 'rename('$IN_y', y)'
	iquery -aq 'rename('$IN_w', w)'

	# do
	for i in $(seq 1 $iter);
	do
		# iter
		time iquery -aq 'store(
		    project(
		    apply(join(w, project(
		    apply(
		    spgemm(transpose(X),
		    project(apply(
			join(spgemm(X, w), y), delta, pow(1+EXP(-multiply), -1) - value), 
		    delta)), norm, 0.0000001*multiply), norm)
		), update, value - norm), update), w_new)'

		# change name
		if [ $i -eq 1 ]
		then
			time iquery -aq 'rename(w, '$IN_w')'
		else
			time iquery -aq 'remove(w)'
		fi

		time iquery -aq 'store(project(apply(w_new, value, update), value), w)'
		time iquery -aq 'remove(w_new)'
	done

	# restore
	iquery -aq 'rename(X, '$IN_X')'
	iquery -aq 'rename(y, '$IN_y')'
}

function pagerank() {
	DATASET=$1
	iter=$2
	
	IN_X='mat_'$DATASET
	IN_v='mat_'$DATASET'_v'

	##############################
	# PageRank
	##############################
	echo "PageRank "$DATASET

	# setup
	iquery -aq 'remove(v)'

	# rename
	iquery -aq 'rename('$IN_X', X)'
	iquery -aq 'rename('$IN_v', v)'

	if [ $DATASET == "enron" ]
	then
		time iquery -aq 'store(build(<value:double NOT NULL>[i=0:36691:0:3670; j=0:0:0:1], (double(1) - 0.85)/36692), with_one)'
	elif [ $DATASET == "epinions" ]
	then
		time iquery -aq 'store(build(<value:double NOT NULL>[i=0:75887:0:7589; j=0:0:0:1], (double(1) - 0.85)/75888), with_one)'
	elif [ $DATASET == "livejournal" ]
	then
		time iquery -aq 'store(build(<value:double NOT NULL>[i=0:4847570:0:484758; j=0:0:0:1], (double(1) - 0.85)/4847571), with_one)'
	elif [ $DATASET == "twitter" ]
	then
		time iquery -aq 'store(build(<value:double NOT NULL>[i=0:61578414:0:6157842; j=0:0:0:1], (double(1) - 0.85)/61578415), with_one)'
	fi
	

	# do
	for i in $(seq 1 $iter);
	do
		# iter
		time iquery -aq 'store(project(apply(project(apply(join(
  			spgemm(X, v) AS a,
			with_one AS b
		), res, (0.85 * a.multiply) + b.value), res), value, res), value), v_new);'

		# change name
		if [ $i -eq 1 ]
		then
			time iquery -aq 'rename(v, '$IN_v')'
		else
			time iquery -aq 'remove(v)'
		fi

		time iquery -aq 'rename(v_new, v)'
	done

	# restore
	iquery -aq 'rename(X, '$IN_X')'

	iquery -aq 'remove(with_one)'
}
