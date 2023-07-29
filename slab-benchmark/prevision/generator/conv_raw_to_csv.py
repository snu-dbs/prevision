import sys
import tqdm

dataset = sys.argv[1]
in_path = sys.argv[2]
out_path = sys.argv[3]

assert(dataset in ['enron', 'epinions', 'livejournal', 'twitter'])

f1 = open(in_path, 'r')
f2 = open(out_path, 'w')

# construct helper
num_nodes = 36692
if dataset == 'epinions':
    num_nodes = 75888
elif dataset == 'livejournal':
    num_nodes = 4847571
elif dataset == 'twitter':
    num_nodes = 61578414

helper = [0 for _ in range(num_nodes)]

# skip headers
if dataset != 'twitter':
    for _ in range(4):
        next(f1)

for line in tqdm.tqdm(f1):
    tokens = line.replace('\n', '').split('\t')
    j = int(tokens[1])
    if dataset == 'twitter':
        j -= 1
    
    helper[j] += 1

# generate CSV files
f1.seek(0)

# skip headers
if dataset != 'twitter':
    for _ in range(4):
        next(f1)

# append values
for line in tqdm.tqdm(f1):
    tokens = line.replace('\n', '').split('\t')
    i, j = int(tokens[0]), int(tokens[1])
    if dataset == 'twitter':
        i -= 1
        j -= 1

    f2.write(f'{i},{j},{1/helper[j]}\n')

f1.close()
f2.close()
