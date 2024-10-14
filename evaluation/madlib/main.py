import sys
import csv
import tqdm
import psycopg2

conn = psycopg2.connect("dbname=pgtile user=pgtile")
cur = conn.cursor()
arrname = sys.argv[2] 
is_vec = int(sys.argv[3]) == 1

if is_vec:
	# cur.execute("DROP TABLE {arrname} ;".format(arrname=arrname))
	cur.execute("CREATE TABLE {arrname} ( vec DOUBLE PRECISION[]);".format(arrname=arrname))

	with open(sys.argv[1], 'r') as f:
	    values='\'{' + ','.join(f.readlines()) + '}\''
	    cur.execute("INSERT INTO {arrname} (vec) VALUES ( {values} );".format(arrname=arrname, values=values))

	conn.commit()
	cur.close()
	conn.close()
	exit()

# cur.execute("DROP TABLE {arrname} ;".format(arrname=arrname))
cur.execute("CREATE TABLE {arrname} ( row_id integer, row_vec DOUBLE PRECISION[]);".format(arrname=arrname))

with open(sys.argv[1], 'r') as f:
    reader = csv.reader(f)
    for idx, items in enumerate(tqdm.tqdm(reader)):
        values='\'{' + ','.join(map(lambda x: str(x), items)) + '}\''
        cur.execute("INSERT INTO {arrname} (row_id, row_vec) VALUES ( {idx}, {values} );".format(arrname=arrname, idx=idx + 1, values=values))

conn.commit()
cur.close()
conn.close()
