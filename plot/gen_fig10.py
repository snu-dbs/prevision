import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from util import collect_time

def render_fig10a(nmf_data):
    x = [1, 2, 4, 8]

    fig = plt.figure(figsize=(5, 3))
    plt.plot(x, np.array(nmf_data[0]), marker='X', color='b')        # PreVision
    plt.plot(x, np.array(nmf_data[1]), marker='s', color='r')        # SystemDS
    plt.plot(x, np.array(nmf_data[2]), marker='o', color='y')        # MLlib
    plt.plot(x, np.array(nmf_data[3]), marker='P', color='m')        # MADlib
    plt.plot(x, np.array(nmf_data[4]), marker='*', color='g')        # SciDB
    plt.plot(x, np.array(nmf_data[5]), marker='^', color='c')        # NumPy
    plt.plot(x, np.array(nmf_data[6]), marker='d', color='k')        # Dask

    plt.xlim(1, 8)
    plt.ylim(10, 10000)
    plt.yscale('log')
    plt.tick_params(axis='both', which='major', labelsize=18)
    plt.ylabel("Elapsed Time (s)", fontdict={'size': 18})

    plt.xlabel("Parallelism", fontdict={'size': 18}, labelpad=12)
    fig.patch.set_facecolor('white')

    plt.grid()
    plt.savefig('output/fig10_a.pdf', format='pdf', bbox_inches='tight')


def render_fig10b(slr_data):
    x = [1, 2, 4, 8]

    fig = plt.figure(figsize=(5, 3))
    plt.plot(x, np.array(slr_data[0]), marker='X', color='b')                   # PreVision
    plt.plot(x, np.array(slr_data[1]), marker='s', color='r')                  # SystemDS
    plt.plot(x, np.array(slr_data[2]), marker='o', color='y')            # MLlib
    plt.plot(x, np.array(slr_data[3]), marker='P', color='m')      # MADlib
    plt.plot([1, 2,], np.array(slr_data[4]), marker='*', color='g')    # SciDB

    plt.xlim(1, 8)
    plt.ylim(10, 100000)
    plt.yscale('log')
    plt.tick_params(axis='both', which='major', labelsize=18)

    plt.xlabel("Parallelism", fontdict={'size': 18}, labelpad=12)

    fig.patch.set_facecolor('white')

    plt.grid()

    plt.savefig('output/fig10_b.pdf', format='pdf', bbox_inches='tight')


def prepare_data():
    fig10_nmf_data = [
        [
            collect_time('time-prevision-nmf-10m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-10m-3-2-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-10m-3-4-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-10m-3-8-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-nmf-10m-3-1.log'),
            collect_time('time-systemds-nmf-10m-3-2.log'),
            collect_time('time-systemds-nmf-10m-3-4.log'),
            collect_time('time-systemds-nmf-10m-3-8.log'),
        ],
        [
            collect_time('time-mllib-nmf-10m-3-1.log'),
            collect_time('time-mllib-nmf-10m-3-2.log'),
            collect_time('time-mllib-nmf-10m-3-4.log'),
            collect_time('time-mllib-nmf-10m-3-8.log'),
        ],
        [
            collect_time('time-madlib-nmf-10m-3-1.log'),
            collect_time('time-madlib-nmf-10m-3-2.log'),
            collect_time('time-madlib-nmf-10m-3-4.log'),
            collect_time('time-madlib-nmf-10m-3-8.log'),
        ],
        [
            collect_time('time-scidb-nmf-10m-3-1.log'),
            collect_time('time-scidb-nmf-10m-3-2.log'),
            collect_time('time-scidb-nmf-10m-3-4.log'),
            collect_time('time-scidb-nmf-10m-3-8.log'),
        ],
        [
            collect_time('time-numpy-nmf-10m-3-1.log'),
            collect_time('time-numpy-nmf-10m-3-2.log'),
            collect_time('time-numpy-nmf-10m-3-4.log'),
            collect_time('time-numpy-nmf-10m-3-8.log'),
        ],
        [
            collect_time('time-dask-nmf-10m-3-1.log'),
            collect_time('time-dask-nmf-10m-3-2.log'),
            collect_time('time-dask-nmf-10m-3-4.log'),
            collect_time('time-dask-nmf-10m-3-8.log'),
        ]
    ]

    fig10_slr_data = [
        [
            collect_time('time-prevision-slr-0.0125-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.0125-3-2-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.0125-3-4-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.0125-3-8-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-slr-0.0125-3-1.log'),
            collect_time('time-systemds-slr-0.0125-3-2.log'),
            collect_time('time-systemds-slr-0.0125-3-4.log'),
            collect_time('time-systemds-slr-0.0125-3-8.log'),
        ],
        [
            collect_time('time-mllib-slr-0.0125-3-1.log'),
            collect_time('time-mllib-slr-0.0125-3-2.log'),
            collect_time('time-mllib-slr-0.0125-3-4.log'),
            collect_time('time-mllib-slr-0.0125-3-8.log'),
        ],
        [
            collect_time('time-madlib-slr-0.0125-3-1.log'),
            collect_time('time-madlib-slr-0.0125-3-2.log'),
            collect_time('time-madlib-slr-0.0125-3-4.log'),
            collect_time('time-madlib-slr-0.0125-3-8.log'),
        ],
        [
            collect_time('time-scidb-slr-0.0125-3-1.log'),
            collect_time('time-scidb-slr-0.0125-3-2.log'),
        ]
    ]

    return fig10_nmf_data, fig10_slr_data

def render_all_fig10():
    fig10_nmf_data, fig10_slr_data = prepare_data()

    plt.rcParams.update(plt.rcParamsDefault)

    plt.style.use('classic')

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "14"

    render_fig10a(fig10_nmf_data)
    render_fig10b(fig10_slr_data)


if __name__ == '__main__':
    render_all_fig10()
