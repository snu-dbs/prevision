source ./venv3/bin/activate

python ./generator/conv_raw_to_csv.py enron ./output/raw/email-Enron.txt ./output/csv/enron.csv
python ./generator/conv_raw_to_csv.py epinions ./output/raw/soc-Epinions1.txt ./output/csv/epinions.csv
python ./generator/conv_raw_to_csv.py livejournal ./output/raw/soc-LiveJournal1.txt ./output/csv/livejournal.csv
python ./generator/conv_raw_to_csv.py twitter ./output/raw/twitter_rv.net ./output/csv/twitter.csv

for i in $(seq 0 36691) 
do
    echo -e "$i,0,0.00002725389" >> ./output/csv/enron_v.csv;
done

for i in $(seq 0 75887) 
do
    echo -e "$i,0,0.00001317748" >> ./output/csv/epinions_v.csv;
done

for i in $(seq 0 4847570) 
do
    echo -e "$i,0,0.000000206288924" >> ./output/csv/livejournal_v.csv;
done

for i in $(seq 0 61578414) 
do
    echo -e "$i,0,0.0000000162394569" >> ./output/csv/twitter_v.csv;
done
