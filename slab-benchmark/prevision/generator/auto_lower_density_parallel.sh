source ./venv3/bin/activate

cat './generator/list_lower_density.txt' | parallel --colsep ' ' -j 4  python ./generator/gen_lower_density.py {1} {2} {3}