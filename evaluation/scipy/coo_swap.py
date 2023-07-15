import os                                   

os.environ["OMP_NUM_THREADS"] = "1"         # export OMP_NUM_THREADS=1
os.environ["OPENBLAS_NUM_THREADS"] = "1"    # export OPENBLAS_NUM_THREADS=1
os.environ["MKL_NUM_THREADS"] = "1"         # export MKL_NUM_THREADS=1
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"  # export VECLIB_MAXIMUM_THREADS=1
os.environ["NUMEXPR_NUM_THREADS"] = "1"     # export NUMEXPR_NUM_THREADS=1

import sys
import time

import numpy as np

from scipy.sparse import coo_matrix, load_npz, save_npz
from scipy.sparse.linalg import norm, expm


def print_elapsed_time(func):
    def wrapper(*args, **kwargs):
        start = time.time_ns()

        func(*args, **kwargs)

        end = time.time_ns()
        print(f'elapsed time: {(end - start)} ns')

    return wrapper


@print_elapsed_time
def eval_TRANS(X_path):
    X = load_npz(X_path)

    RES = X.transpose()

    save_npz('TRANS_RES', RES, compressed=False)
    os.sync()

@print_elapsed_time
def eval_NORM(X_path):
    X = load_npz(X_path)

    RES = norm(X)
    print(RES)


@print_elapsed_time
def eval_GRM(X_path):
    X = load_npz(X_path)

    RES = X.transpose().dot(X)

    save_npz('GRM_RES', RES, compressed=False)
    os.sync()


@print_elapsed_time
def eval_MVM(X_path, w_path):
    X = load_npz(X_path)
    w = np.load(w_path)

    RES = X.dot(w)

    # save_npz('MVM_RES', RES, compressed=False)
    np.save('MVM_RES', RES)
    os.sync()


@print_elapsed_time
def eval_ADD(M_path, N_path):
    M = load_npz(M_path)
    N = load_npz(N_path)

    RES = M + N

    save_npz('ADD_RES', RES, compressed=False)
    os.sync()


@print_elapsed_time
def eval_GMM(M_path, N_path):
    M = load_npz(M_path)
    N = load_npz(N_path)

    RES = M.dot(N)

    save_npz('GMM_RES', RES, compressed=False)
    os.sync()


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

