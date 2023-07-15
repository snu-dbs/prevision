import os                                   

os.environ["OMP_NUM_THREADS"] = "1"         # export OMP_NUM_THREADS=1
os.environ["OPENBLAS_NUM_THREADS"] = "1"    # export OPENBLAS_NUM_THREADS=1
os.environ["MKL_NUM_THREADS"] = "1"         # export MKL_NUM_THREADS=1
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"  # export VECLIB_MAXIMUM_THREADS=1
os.environ["NUMEXPR_NUM_THREADS"] = "1"     # export NUMEXPR_NUM_THREADS=1

import sys
import time

import numpy as np


def print_elapsed_time(func):
    def wrapper(*args, **kwargs):
        start = time.time_ns()

        func(*args, **kwargs)

        end = time.time_ns()
        print(f'elapsed time: {(end - start)} ns')

    return wrapper


# @print_elapsed_time
def eval_TRANS(X_path):
    X_start = time.time_ns()
    X = np.load(X_path)
    X_end = time.time_ns()

    TRANS_start = time.time_ns()
    RES = np.transpose(X)
    TRANS_end = time.time_ns()

    save_start = time.time_ns()
    np.save('TRANS_RES', RES)
    os.sync()
    save_end = time.time_ns()

    print(f'{(X_end - X_start)},{(TRANS_end - TRANS_start)},{(save_end - save_start)}')


# @print_elapsed_time
def eval_NORM(X_path):
    X_start = time.time_ns()
    X = np.load(X_path)
    X_end = time.time_ns()

    norm_start = time.time_ns()
    res = np.linalg.norm(X)
    norm_end = time.time_ns()

    print(f'{(X_end - X_start)},{(norm_end - norm_start)}')

# @print_elapsed_time
def eval_GRM(X_path):
    X_start = time.time_ns()
    X = np.load(X_path)
    X_end = time.time_ns()

    OP_start = time.time_ns()
    RES = np.dot(np.transpose(X), X)
    OP_end = time.time_ns()

    save_start = time.time_ns()
    np.save('GRM_RES', RES)
    os.sync()
    save_end = time.time_ns()

    print(f'{(X_end - X_start)},{(OP_end - OP_start)},{(save_end - save_start)}')


# @print_elapsed_time
def eval_MVM(X_path, w_path):
    X_start = time.time_ns()
    X = np.load(X_path)
    w = np.load(w_path)
    X_end = time.time_ns()

    OP_start = time.time_ns()
    RES = np.dot(X, w)
    OP_end = time.time_ns()

    save_start = time.time_ns()
    np.save('MVM_RES', RES)
    os.sync()
    save_end = time.time_ns()

    print(f'{(X_end - X_start)},{(OP_end - OP_start)},{(save_end - save_start)}')


# @print_elapsed_time
def eval_ADD(M_path, N_path):
    X_start = time.time_ns()
    M = np.load(M_path)
    N = np.load(N_path)
    X_end = time.time_ns()

    OP_start = time.time_ns()
    RES = np.add(M, N)
    OP_end = time.time_ns()

    save_start = time.time_ns()
    np.save('ADD_RES', RES)
    os.sync()
    save_end = time.time_ns()

    print(f'{(X_end - X_start)},{(OP_end - OP_start)},{(save_end - save_start)}')


# @print_elapsed_time
def eval_GMM(M_path, N_path):
    os.sync()

    X_start = time.time_ns()
    M = np.load(M_path)
    N = np.load(N_path)
    X_end = time.time_ns()

    OP_start = time.time_ns()
    RES = np.dot(M, N)
    OP_end = time.time_ns()

    save_start = time.time_ns()
    np.save('GMM_RES', RES)
    os.sync()
    save_end = time.time_ns()

    print(f'{(X_end - X_start)},{(OP_end - OP_start)},{(save_end - save_start)}')


def test():
    op = sys.argv[1]
    opnd_1 = sys.argv[2]
    opnd_2 = sys.argv[3] if len(sys.argv) >= 4 else None

    if (op == 'TRANS'):
        eval_TRANS(opnd_1)
    elif (op == 'NORM'):
        eval_NORM(opnd_1)
    elif (op == 'GRM'):
        eval_GRM(opnd_1)
    elif (op == 'MVM'):
        eval_MVM(opnd_1, opnd_2)
    elif (op == 'ADD'):
        eval_ADD(opnd_1, opnd_2)
    elif (op == 'GMM'):
        eval_GMM(opnd_1, opnd_2)


if __name__ == '__main__':
    test()

