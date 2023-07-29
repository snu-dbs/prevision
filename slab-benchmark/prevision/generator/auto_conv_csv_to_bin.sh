source ./venv2/bin/activate

if [ ! -d "./output/ijv" ]; then
    mkdir -p "./output/ijv"
fi

if [ ! -d "./output/sysds" ]; then
    mkdir -p "./output/sysds"
fi

# Generate dense matrices for LR and NMF
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/10000000x100_dense.csv O=./output/sysds/10000000x100_dense row=100000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/10000000x10_dense.csv O=./output/sysds/10000000x10_dense row=100000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/10000000x1_dense.csv O=./output/sysds/10000000x1_dense row=100000

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/20000000x100_dense.csv O=./output/sysds/20000000x100_dense row=200000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/20000000x10_dense.csv O=./output/sysds/20000000x10_dense row=200000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/20000000x1_dense.csv O=./output/sysds/20000000x1_dense row=200000

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/40000000x100_dense.csv O=./output/sysds/40000000x100_dense row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/40000000x10_dense.csv O=./output/sysds/40000000x10_dense row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/40000000x1_dense.csv O=./output/sysds/40000000x1_dense row=400000

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/80000000x100_dense.csv O=./output/sysds/80000000x100_dense row=800000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/80000000x10_dense.csv O=./output/sysds/80000000x10_dense row=800000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/80000000x1_dense.csv O=./output/sysds/80000000x1_dense row=800000

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/100x1_dense.csv O=./output/sysds/100x1_dense row=100
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/csv/10x100_dense.csv O=./output/sysds/10x100_dense row=100

# Generate sparse matrices for LR
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x100_sparse_0.0125.csv > ./output/ijv/400000000x100_sparse_0.0125.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x1_sparse_0.0125.csv > ./output/ijv/400000000x1_sparse_0.0125.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/100x1_sparse_0.0125.csv > ./output/ijv/100x1_sparse_0.0125.ijv

cp ./output/csv/400000000x100_sparse_0.0125.csv.mtd ./output/ijv/400000000x100_sparse_0.0125.ijv.mtd
cp ./output/csv/400000000x1_sparse_0.0125.csv.mtd ./output/ijv/400000000x1_sparse_0.0125.ijv.mtd
cp ./output/csv/100x1_sparse_0.0125.csv.mtd ./output/ijv/100x1_sparse_0.0125.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x100_sparse_0.0125.ijv O=./output/sysds/400000000x100_sparse_0.0125 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x1_sparse_0.0125.ijv O=./output/sysds/400000000x1_sparse_0.0125 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/100x1_sparse_0.0125.ijv O=./output/sysds/100x1_sparse_0.0125 row=100

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x100_sparse_0.025.csv > ./output/ijv/400000000x100_sparse_0.025.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x1_sparse_0.025.csv > ./output/ijv/400000000x1_sparse_0.025.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/100x1_sparse_0.025.csv > ./output/ijv/100x1_sparse_0.025.ijv

cp ./output/csv/400000000x100_sparse_0.025.csv.mtd ./output/ijv/400000000x100_sparse_0.025.ijv.mtd
cp ./output/csv/400000000x1_sparse_0.025.csv.mtd ./output/ijv/400000000x1_sparse_0.025.ijv.mtd
cp ./output/csv/100x1_sparse_0.025.csv.mtd ./output/ijv/100x1_sparse_0.025.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x100_sparse_0.025.ijv O=./output/sysds/400000000x100_sparse_0.025 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x1_sparse_0.025.ijv O=./output/sysds/400000000x1_sparse_0.025 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/100x1_sparse_0.025.ijv O=./output/sysds/100x1_sparse_0.025 row=100

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x100_sparse_0.05.csv > ./output/ijv/400000000x100_sparse_0.05.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x1_sparse_0.05.csv > ./output/ijv/400000000x1_sparse_0.05.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/100x1_sparse_0.05.csv > ./output/ijv/100x1_sparse_0.05.ijv

cp ./output/csv/400000000x100_sparse_0.05.csv.mtd ./output/ijv/400000000x100_sparse_0.05.ijv.mtd
cp ./output/csv/400000000x1_sparse_0.05.csv.mtd ./output/ijv/400000000x1_sparse_0.05.ijv.mtd
cp ./output/csv/100x1_sparse_0.05.csv.mtd ./output/ijv/100x1_sparse_0.05.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x100_sparse_0.05.ijv O=./output/sysds/400000000x100_sparse_0.05 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x1_sparse_0.05.ijv O=./output/sysds/400000000x1_sparse_0.05 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/100x1_sparse_0.05.ijv O=./output/sysds/100x1_sparse_0.05 row=100

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x100_sparse_0.1.csv > ./output/ijv/400000000x100_sparse_0.1.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/400000000x1_sparse_0.1.csv > ./output/ijv/400000000x1_sparse_0.1.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/100x1_sparse_0.1.csv > ./output/ijv/100x1_sparse_0.1.ijv

cp ./output/csv/400000000x100_sparse_0.1.csv.mtd ./output/ijv/400000000x100_sparse_0.1.ijv.mtd
cp ./output/csv/400000000x1_sparse_0.1.csv.mtd ./output/ijv/400000000x1_sparse_0.1.ijv.mtd
cp ./output/csv/100x1_sparse_0.1.csv.mtd ./output/ijv/100x1_sparse_0.1.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x100_sparse_0.1.ijv O=./output/sysds/400000000x100_sparse_0.1 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/400000000x1_sparse_0.1.ijv O=./output/sysds/400000000x1_sparse_0.1 row=400000
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/100x1_sparse_0.1.ijv O=./output/sysds/100x1_sparse_0.1 row=100

exit;


# Generate sparse matrices for PageRank
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/enron.csv > ./output/ijv/enron.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/enron_v.csv > ./output/ijv/enron_v.ijv

cp ./output/csv/enron.csv.mtd ./output/ijv/enron.ijv.mtd
cp ./output/csv/enron_v.csv.mtd ./output/ijv/enron_v.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/enron.ijv O=./output/sysds/enron row=3670
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/enron_v.ijv O=./output/sysds/enron_v row=3670

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/epinions.csv > ./output/ijv/epinions.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/epinions_v.csv > ./output/ijv/epinions_v.ijv

cp ./output/csv/epinions.csv.mtd ./output/ijv/epinions.ijv.mtd
cp ./output/csv/epinions_v.csv.mtd ./output/ijv/epinions_v.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/epinions.ijv O=./output/sysds/epinions row=7588
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/epinions_v.ijv O=./output/sysds/epinions_v row=7588

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/livejournal.csv > ./output/ijv/livejournal.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/livejournal_v.csv > ./output/ijv/livejournal_v.ijv

cp ./output/csv/livejournal.csv.mtd ./output/ijv/livejournal.ijv.mtd
cp ./output/csv/livejournal_v.csv.mtd ./output/ijv/livejournal_v.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/livejournal.ijv O=./output/sysds/livejournal row=484758
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/livejournal_v.ijv O=./output/sysds/livejournal_v row=484758

awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/twitter.csv > ./output/ijv/twitter.ijv
awk -F ',' '{print $1+1 " " $2+1 " " $3 }' ./output/csv/twitter_v.csv > ./output/ijv/twitter_v.ijv

cp ./output/csv/twitter.csv.mtd ./output/ijv/twitter.ijv.mtd
cp ./output/csv/twitter_v.csv.mtd ./output/ijv/twitter_v.ijv.mtd

systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/twitter.ijv O=./output/sysds/twitter row=6157842
systemds -f ./generator/conv_csv_to_bin.dml -nvargs X=./output/ijv/twitter_v.ijv O=./output/sysds/twitter_v row=6157842
