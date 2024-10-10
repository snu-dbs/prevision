import os                                   

os.environ["OMP_NUM_THREADS"] = os.environ["PARALLELISM"]
os.environ["OPENBLAS_NUM_THREADS"] = os.environ["PARALLELISM"]
os.environ["MKL_NUM_THREADS"] = os.environ["PARALLELISM"]
os.environ["VECLIB_MAXIMUM_THREADS"] = os.environ["PARALLELISM"]
os.environ["NUMEXPR_NUM_THREADS"] = os.environ["PARALLELISM"]

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


@print_elapsed_time
def eval_TRANS(X_path):
    X = np.load(X_path, mmap_mode='r')
    RES = np.memmap('TRANS_RES', dtype='double', mode='w+', shape=(X.shape[1], X.shape[0]))

    RES[:] = np.transpose(X)[:]

    RES.flush()


@print_elapsed_time
def eval_NORM(X_path):
    X = np.load(X_path, mmap_mode='r')

    res = np.linalg.norm(X)
    print(res)


@print_elapsed_time
def eval_GRM(X_path):
    X = np.load(X_path, mmap_mode='r')
    RES = np.memmap('GRM_RES', dtype='double', mode='w+', shape=(X.shape[1], X.shape[1]))

    np.dot(np.transpose(X), X, out=RES)

    RES.flush()


@print_elapsed_time
def eval_MVM(X_path, w_path):
    X = np.load(X_path, mmap_mode='r')
    w = np.load(w_path, mmap_mode='r')
    RES = np.memmap('MVM_RES', dtype='double', mode='w+', shape=(X.shape[0], 1))

    np.dot(X, w, out=RES)

    RES.flush()


@print_elapsed_time
def eval_ADD(M_path, N_path):
    M = np.load(M_path, mmap_mode='r')
    N = np.load(N_path, mmap_mode='r')
    RES = np.memmap('ADD_RES', dtype='double', mode='w+', shape=(M.shape[0], M.shape[1]))

    np.add(M, N, out=RES)

    RES.flush()


@print_elapsed_time
def eval_GMM(M_path, N_path):
    M = np.load(M_path, mmap_mode='r')
    N = np.load(N_path, mmap_mode='r')
    RES = np.memmap('GMM_RES', dtype='double', mode='w+', shape=(M.shape[0], N.shape[1]))

    np.dot(M, N, out=RES)

    RES.flush()


@print_elapsed_time
def eval_MANY_ADD(M_path, N_path, O_path, P_path, Q_path):
    M = np.load(M_path, mmap_mode='r')
    N = np.load(N_path, mmap_mode='r')
    O = np.load(O_path, mmap_mode='r')
    P = np.load(P_path, mmap_mode='r')
    Q = np.load(Q_path, mmap_mode='r')

    MN = np.memmap('KOO_MN', dtype='double', mode='w+', shape=(M.shape[0], M.shape[1]))
    OP = np.memmap('KOO_OP', dtype='double', mode='w+', shape=(M.shape[0], M.shape[1]))
    MNOP = np.memmap('KOO_MNOP', dtype='double', mode='w+', shape=(M.shape[0], M.shape[1]))
    RES = np.memmap('KOO_ADD_RES', dtype='double', mode='w+', shape=(M.shape[0], M.shape[1]))

    np.add(M, N, out=MN)
    np.add(O, P, out=OP)
    np.add(MN, OP, out=MNOP)
    np.add(MNOP, Q, out=RES)

    RES.flush()


@print_elapsed_time
def eval_NMF(X_path, W_path, H_path, iter=3):
    X = np.load(X_path, mmap_mode='r')
    W = np.load(W_path, mmap_mode='r+')
    H = np.load(H_path, mmap_mode='r+')

    # Hmmm..... ?
    XHt = np.memmap('__XHt', dtype='double', mode='w+', shape=(X.shape[0], H.shape[0]))
    WH = np.memmap('__WH', dtype='double', mode='w+', shape=(W.shape[0], H.shape[1]))
    WHHt = np.memmap('__WHHt', dtype='double', mode='w+', shape=(WH.shape[0], H.shape[0]))
    W_RHS = np.memmap('__W_RHS', dtype='double', mode='w+', shape=WHHt.shape)

    WtX = np.memmap('__WtX', dtype='double', mode='w+', shape=(W.shape[1], X.shape[1]))
    WtW = np.memmap('__WtW', dtype='double', mode='w+', shape=(W.shape[1], W.shape[1]))
    WtWH = np.memmap('__WtWH', dtype='double', mode='w+', shape=(WtW.shape[0], H.shape[1]))
    H_RHS = np.memmap('__H_RHS', dtype='double', mode='w+', shape=WtWH.shape)

    # elemwise_time, transpose_time, matmul_time = 0, 0, 0
    for i in range(iter):
        # np.multiply(W, (X.dot(H.T) / W.dot(H).dot(H.T)), out=W)
        Ht = H.T
        np.dot(X, Ht, out=XHt)
        np.dot(W, H, out=WH)
        np.dot(WH, Ht, out=WHHt)
        np.divide(XHt, WHHt, out=W_RHS)
        np.multiply(W, W_RHS, out=W)
        
        
        # np.multiply(H, (W.T.dot(X) / W.T.dot(W).dot(H)), out=H)
        Wt = W.T
        np.dot(Wt, X, out=WtX)
        np.dot(Wt, W, out=WtW)
        np.dot(WtW, H, out=WtWH)
        np.divide(WtX, WtWH, out=H_RHS)
        np.multiply(H, H_RHS, out=H)

        # continue
        # rmse = np.sqrt(np.power(np.dot(W, H) - X, 2).sum() / (X.shape[0] * X.shape[1]))
        # print(f'{i},rmse={rmse}')

    W.flush()
    H.flush()
    
    os.sync()


@print_elapsed_time
def eval_LR(X_path, y_path, w_path, _iter=3, alpha=0.0001):
    # (memmap)
    X = np.load(X_path, mmap_mode='r')
    y = np.load(y_path, mmap_mode='r')
    w = np.load(w_path, mmap_mode='r+')
    # print(w.shape)

    Xw = np.memmap('__Xw', dtype='double', mode='w+', shape=(X.shape[0], w.shape[1]))
    mXw = np.memmap('__mXw', dtype='double', mode='w+', shape=(Xw.shape[0], Xw.shape[1]))
    expmXw = np.memmap('__expmXw', dtype='double', mode='w+', shape=(mXw.shape[0], mXw.shape[1]))
    oexpmXw = np.memmap('__oexpmXw', dtype='double', mode='w+', shape=(expmXw.shape[0], expmXw.shape[1]))
    ylhs = np.memmap('__ylhs', dtype='double', mode='w+', shape=(oexpmXw.shape[0], oexpmXw.shape[1]))
    yDiff = np.memmap('__yDiff', dtype='double', mode='w+', shape=(ylhs.shape[0], ylhs.shape[1]))
    XtyDiff = np.memmap('__XtyDiff', dtype='double', mode='w+', shape=(X.shape[1], yDiff.shape[1]))
    wrhs = np.memmap('__wrhs', dtype='double', mode='w+', shape=w.shape)

    for _i in range(_iter):
        # fitting
        # w = w - alpha * X.T @ ((1 / (1 + np.exp(-1 * X @ w))) - y)
        np.dot(X, w, out=Xw)
        np.multiply(-1, Xw, out=mXw)
        np.exp(mXw, out=expmXw)
        np.add(1, expmXw, out=oexpmXw)
        np.divide(1, oexpmXw, out=ylhs)

        np.subtract(ylhs, y, out=yDiff)
        np.dot(X.T, yDiff, out=XtyDiff)
        np.multiply(alpha, XtyDiff, out=wrhs)
        np.subtract(w, wrhs, out=w)
       
        # continue
        # RMSE
        # rmse = np.sqrt(np.power(((1 / (1 + np.exp(-1 * X @ w))) - y), 2).sum() / (y.shape[0] * y.shape[1]))
        # print(f'{_i}, rmse={rmse}')

    w.flush()
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
    elif (op == 'MANYADD'):
        eval_MANY_ADD(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    elif (op == 'NMF'):
        assert(len(sys.argv) == 6)
        X, W, H = sys.argv[2], sys.argv[3], sys.argv[4]
        max_iter = int(sys.argv[5])
        eval_NMF(X, W, H, max_iter)    # rank is assumed to 10
    elif (op == 'LR'):
        assert(len(sys.argv) == 7)
        X, y, w = sys.argv[2], sys.argv[3], sys.argv[4]
        max_iter, alpha = int(sys.argv[5]), float(sys.argv[6])
        eval_LR(X, y, w, max_iter, alpha)


if __name__ == '__main__':
    test()

