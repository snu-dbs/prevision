BASE='../evaluation/results'

def collect_time(path: str) -> float:
    try:
        with open(BASE + '/' + path, 'r') as f:
            _sum = 0
            _cnt = 0
            for line in f.readlines():
                _sum += float(line)
                _cnt += 1
        return _sum / _cnt
    except:
        return 0

def collect_io(path: str) -> float:
    try:
        with open(BASE + '/' + path, 'r') as f:
            lines = f.readlines()
            if len(lines) == 0:
                return 0, 0

            last = lines[-1]
            items = last.split(',')
            return int(items[0]), int(items[1])
    except Exception as e:
        return 0, 0

def collect_breakdown(path: str) -> float:
    try:
        with open(BASE + '/' + path, 'r') as f:
            lines = f.readlines()
            if len(lines) == 0:
                return 0, 0, 0, 0

            last = lines[-2]	# since there is a bug in gawk script
            items = last.split(',')
            return int(items[1]), int(items[3]), int(items[2]), int(items[0])
    except:
        return 0, 0, 0, 0
