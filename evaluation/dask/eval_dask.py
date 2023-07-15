import os                                   

os.environ["OMP_NUM_THREADS"] = "1"         # export OMP_NUM_THREADS=1
os.environ["OPENBLAS_NUM_THREADS"] = "1"    # export OPENBLAS_NUM_THREADS=1
os.environ["MKL_NUM_THREADS"] = "1"         # export MKL_NUM_THREADS=1
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"  # export VECLIB_MAXIMUM_THREADS=1
os.environ["NUMEXPR_NUM_THREADS"] = "1"     # export NUMEXPR_NUM_THREADS=1

import sys
import time

import h5py

import dask.array as da
import time

from dask.distributed import Client


def print_elapsed_time(func):
    def wrapper(*args, **kwargs):
        start = time.time_ns()

        func(*args, **kwargs)

        end = time.time_ns()
        print(f'elapsed time: {(end - start)} ns')

    return wrapper


@print_elapsed_time
def eval_TRANS(X_path):
    dx = h5py.File(X_path)['/data']
    X = da.from_array(dx, chunks=dx.chunks)

    RES = X.T

    RES.to_hdf5('TRANS_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_NORM(X_path):
    dx = h5py.File(X_path)['/data']
    X = da.from_array(dx, chunks=dx.chunks)

    res = da.linalg.norm(X).compute()
    # print(res)


@print_elapsed_time
def eval_GRM(X_path):
    dx = h5py.File(X_path)['/data']
    X = da.from_array(dx, chunks=dx.chunks)

    RES = da.dot(X.T, X)

    RES.to_hdf5('GRM_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_MVM(X_path, w_path):
    dx = h5py.File(X_path)['/data']
    dw = h5py.File(w_path)['/data']
    X = da.from_array(dx, chunks=dx.chunks)

    # I did not use explicit chunk size due to the performance warning
    # w = da.from_array(dw, chunks=dw.chunks)   
    w = da.from_array(dw)

    RES = da.dot(X, w)

    RES.to_hdf5('MVM_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_ADD(M_path, N_path):
    dm = h5py.File(M_path)['/data']
    dn = h5py.File(N_path)['/data']
    M = da.from_array(dm, chunks=dm.chunks)
    N = da.from_array(dn, chunks=dn.chunks)

    RES = da.add(M, N)

    RES.to_hdf5('ADD_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_GMM(M_path, N_path):
    dm = h5py.File(M_path)['/data']
    dn = h5py.File(N_path)['/data']
    M = da.from_array(dm, chunks=dm.chunks)
    N = da.from_array(dn, chunks=dn.chunks)

    RES = da.dot(M, N)

    RES.to_hdf5('GMM_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_MANY_ADD(M_path, N_path, O_path, P_path, Q_path):
    dm = h5py.File(M_path)['/data']
    dn = h5py.File(N_path)['/data']
    do = h5py.File(O_path)['/data']
    dp = h5py.File(P_path)['/data']
    dq = h5py.File(Q_path)['/data']

    M = da.from_array(dm, chunks=dm.chunks)
    N = da.from_array(dn, chunks=dn.chunks)
    O = da.from_array(do, chunks=do.chunks)
    P = da.from_array(dp, chunks=dp.chunks)
    Q = da.from_array(dq, chunks=dq.chunks)

    MN = da.add(M, N)
    OP = da.add(O, P)
    MNOP = da.add(MN, OP)
    RES = da.add(MNOP, Q)

    RES.to_hdf5('MANYADD_RES.hdf5', '/data')
    os.sync()


@print_elapsed_time
def eval_NMF(X_path, W_path, H_path, iter=3, rank=100):
    dx = h5py.File(X_path)['/data']
    X = da.from_array(dx, chunks=dx.chunks)

    dw = h5py.File(W_path)['/data']
    dh = h5py.File(H_path)['/data']
    W = da.from_array(dw, chunks=dx.chunks)
    H = da.from_array(dh, chunks=dx.chunks)

    for i in range(iter):
        # np.multiply(W, (X.dot(H.T) / W.dot(H).dot(H.T)), out=W)
        Ht = H.T
        XHt = X.dot(Ht)
        WH = W.dot(H)
        WHHt = WH.dot(Ht)
        W_RHS = XHt / WHHt
        W = W * W_RHS
        # W.compute()

        # np.multiply(H, (W.T.dot(X) / W.T.dot(W).dot(H)), out=H)
        Wt = W.T
        WtX = Wt.dot(X)
        WtW = Wt.dot(W)
        WtWH = WtW.dot(H)
        H_RHS = WtX / WtWH
        H = H * H_RHS
        # H.compute()
        continue

    # W.compute()
    H.compute()

    '''
    rmse = da.sqrt(da.power((W.dot(H) - X), 2).sum() / (X.shape[0] * X.shape[1]))
    _rmse = rmse.compute()
    print(f'rmse={_rmse}')
    '''

    W.to_hdf5('__W.hdf5', '/data')
    H.to_hdf5('__H.hdf5', '/data')
    os.sync()

@print_elapsed_time
def eval_LR(X_path, y_path, w_path, _iter=3, alpha=0.0001):
    dx = h5py.File(X_path)['/data']
    dy = h5py.File(y_path)['/data']
    dw = h5py.File(w_path)['/data']

    X = da.from_array(dx, chunks=dx.chunks)
    y = da.from_array(dy, chunks=dy.chunks)
    w = da.from_array(dw, chunks=dw.chunks)

    for _i in range(_iter):
        # fitting
        w = w - alpha * X.T @ ((1 / (1 + da.exp(-1 * X @ w))) - y)
   
        # RMSE
        # rmse = da.sqrt(da.power(((1 / (1 + da.exp(-1 * X @ w))) - y), 2).sum() / (y.shape[0] * y.shape[1])).compute()
        # print(f'{_i}, rmse={rmse}')

    w.compute()

    w.to_hdf5('__w.hdf5', '/data')
    os.sync()


def initialize_client():
    client = Client(processes=False, n_workers=1, threads_per_worker=1)


def test():
    op = sys.argv[1]
    opnd_1 = sys.argv[2]
    opnd_2 = sys.argv[3] if len(sys.argv) >= 4 else None

    initialize_client()

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
        eval_NMF(opnd_1, opnd_2, sys.argv[4], int(sys.argv[5]), 10)
    elif (op == 'LR'):
        eval_LR(opnd_1, opnd_2, sys.argv[4], int(sys.argv[5]), 0.0000001)


if __name__ == '__main__':
    test()

