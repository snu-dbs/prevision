#################################
# Usage:
#  ./exp.sh ${test_size}
#################################

#! /bin/bash

#################################
#           Matmul
#################################
# echo -e "#########################"
# echo Dense Matmul
# echo -e "#########################"
# for i in {1..10}
# do
# ./bin/array_generator dense binary 20000
# sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
# ./bin/lamperf_test_matmul 20000
# rm -rf __*
# done

echo -e "#########################"
echo Sparse Matmul with Sparsity 1
echo -e "#########################"
for i in {1..10}
do
./bin/array_generator sparse binary 20000 1
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
./bin/lamperf_test_matmul 20000
rm -rf __*
done

echo -e "#########################"
echo Sparse Matmul with Sparsity 2
echo -e "#########################"
for i in {1..10}
do
./bin/array_generator sparse binary 20000 2
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
./bin/lamperf_test_matmul 20000
rm -rf __*
done

echo -e "#########################"
echo Sparse Matmul with Sparsity 5
echo -e "#########################"
for i in {1..10}
do
./bin/array_generator sparse binary 20000 5
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
./bin/lamperf_test_matmul 20000
rm -rf __*
done

echo -e "#########################"
echo Sparse Matmul with Sparsity 10
echo -e "#########################"
for i in {1..10}
do
./bin/array_generator sparse binary 20000 10
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
./bin/lamperf_test_matmul 20000
rm -rf __*
done

echo -e "#########################"
echo Sparse Matmul with Sparsity 20
echo -e "#########################"
for i in {1..10}
do
./bin/array_generator sparse binary 20000 20
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
./bin/lamperf_test_matmul 20000
rm -rf __*
done


