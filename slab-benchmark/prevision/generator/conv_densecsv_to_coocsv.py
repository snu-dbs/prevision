import sys
import csv
import tqdm


def main(in_path, out_path):
    print(in_path)
    print(out_path)
    fin = open(in_path, 'r')
    fout = open(out_path, 'w')

    reader = csv.reader(fin)
    for row_idx, rows in enumerate(tqdm.tqdm(reader)):
        for col_idx, row in enumerate(rows):
            fout.write('{},{},{}\n'.format(int(row_idx), int(col_idx), float(row)))

    fin.close()
    fout.close()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
