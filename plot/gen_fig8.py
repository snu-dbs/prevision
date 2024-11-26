import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from util import collect_time

def render(ax, theme, label, data, title, hide_ylabel=False, _type=1, ylabel_type='s', hint=None, xticks=['8GB','16GB','32GB','64GB'], xlabel=""):
    X = np.arange(4)
    width = 0.12
    bars, meta = None, []
    for idx, item in enumerate(data):
        bar = ax.bar(X + (width * idx), item, width=width, color=theme[idx][0], hatch=theme[idx][1], edgecolor='black', label=label[idx])
        bars = bars + bar if bars is not None else bar
    
    for rect in bars:
        if rect.get_height() > 0:
            continue
        
        if _type == 1:
            x = rect.get_x() + (rect.get_width() / 2.0) - 0.02
            y = rect.get_y() + 1.4
            if rect.get_height() == 0:
                ax.text(x, y, "out-of-memory error", {'size': 6, 'color': 'red'}, rotation=90, weight='bold')
            else:
                ax.text(x, y, "timeout", {'size': 6, 'color': 'red'}, rotation=90, weight='bold')
        else:
            x = rect.get_x() + (rect.get_width() / 2.0) - 0.03
            y = rect.get_y() + 1.4
            if rect.get_height() == 0:
                if hint == 'pr':
                    y += 0.3
                ax.text(x, y, "out-of-memory error", {'size': 10, 'color': 'red'}, rotation=90, weight='bold')
            else:
                if hint == 'pr':
                    y += 0.3
                ax.text(x, y, "timeout", {'size': 10, 'color': 'red'}, rotation=90, weight='bold')

    ax.set_xticks(X + 0.32, xticks)
    if hide_ylabel is False:
        if ylabel_type == 's':
            ax.set_ylabel('Elapsed Time (s)', fontsize=14)
        elif ylabel_type == 'ms':
            ax.set_ylabel('Elapsed Time (ms)', fontsize=14)

    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color='#CCCCCC')
    ax.set_yscale('log')
    ax.set_ylim(bottom=1)

    return bars


def render_fig8_dense(lr_data, nmf_data):
    theme = [
        ['black', ''],
        ['white', '\\\\\\\\'],
        ['white', '++++'],
        ['white', '////'],
        ['white', ''],
        ['white', 'xxxx'],
        ['white', '----'],
    ]
    label = ['PreVision', 'SystemDS', 'MLlib', 'MADlib', 'SciDB', 'NumPy', 'Dask']


    # main plot
    # Dense LR
    fig = plt.figure(figsize=(6, 2.4))
    ax1 = fig.add_axes([0, 0, 1, 1])
    ax1.set_ylim(1, 100000)
    ax1.set_yticks([1, 10, 100, 1000, 10000, 100000])
    render(ax1, theme, label, lr_data, "Dense LR", xticks=[
           "10M", "20M", "40M", "80M"], xlabel='The Number of Rows')
    plt.savefig('output/fig8_dense_lr.pdf', format='pdf', bbox_inches='tight')

    # legend
    lfig = plt.figure(figsize=(6, 0.6))
    plt.figlegend(*ax1.get_legend_handles_labels(),
                  loc='upper center', ncol=4, mode="expand", fontsize=11)
    lfig.savefig('output/fig8_legend.pdf', format='pdf', bbox_inches='tight')

    # Dense NMF
    fig = plt.figure(figsize=(6, 2.4))
    ax1 = fig.add_axes([0, 0, 1, 1])
    ax1.set_ylim(1, 100000)
    ax1.set_yticks([1, 10, 100, 1000, 10000, 100000])
    render(ax1, theme, label, nmf_data, "Dense NMF", xticks=[
           "10M", "20M", "40M", "80M"], xlabel='The Number of Rows')
    plt.savefig('output/fig8_dense_nmf.pdf', format='pdf', bbox_inches='tight')


def render_fig8_sparse(slr_data, pr_data):
    theme = [
        ['black', ''],
        ['white', '\\\\\\\\'],
        ['white', '++++'],
        ['white', '////'],
        ['white', ''],
    ]
    label = ['PreVision', 'SystemDS', 'MLlib', 'MADlib', 'SciDB',]


    # sparse LR
    fig = plt.figure(figsize=(6, 2.4))
    ax1 = fig.add_axes([0, 0, 1, 1])
    ax1.set_ylim(1, 100000)
    ax1.set_yticks([1, 10, 100, 1000, 10000, 100000])
    render(ax1, theme, label, slr_data, "Sparse LR", _type=2, xticks=[
           "0.0125", "0.025", "0.05", "0.1"], xlabel='Density', ylabel_type='s')
    plt.savefig('output/fig8_sparse_lr.pdf', format='pdf', bbox_inches='tight')

    # pagerank
    fig = plt.figure(figsize=(6, 2.4))
    ax1 = fig.add_axes([0, 0, 1, 1])
    ax1.set_ylim(1, 100000000)
    ax1.set_yticks([1, 100, 10000, 1000000, 100000000])
    render(ax1, theme, label, np.array(pr_data) * 1000, "PageRank", _type=2, ylabel_type='ms',
                 hint='pr', xticks=['Enron', 'Epinions', 'Livejournal', 'Twitter'], 
                 xlabel='Dataset Label')
    plt.savefig('output/fig8_sparse_pr.pdf', format='pdf', bbox_inches='tight')


def prepare_data():
    fig8_dense_lr_data = [
        [
            collect_time('time-prevision-lr-10m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-lr-20m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-lr-40m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-lr-80m-3-1-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-lr-10m-3-1.log'),
            collect_time('time-systemds-lr-20m-3-1.log'),
            collect_time('time-systemds-lr-40m-3-1.log'),
            collect_time('time-systemds-lr-80m-3-1.log'),
        ],
        [
            collect_time('time-mllib-lr-10m-3-1.log'),
            collect_time('time-mllib-lr-20m-3-1.log'),
            collect_time('time-mllib-lr-40m-3-1.log'),
            collect_time('time-mllib-lr-80m-3-1.log'),
        ],
        [
            collect_time('time-madlib-lr-10m-3-1.log'),
            collect_time('time-madlib-lr-20m-3-1.log'),
            collect_time('time-madlib-lr-40m-3-1.log'),
            collect_time('time-madlib-lr-80m-3-1.log'),
        ],
        [ 
            collect_time('time-scidb-lr-10m-3-1.log'),
            collect_time('time-scidb-lr-20m-3-1.log'),
            collect_time('time-scidb-lr-40m-3-1.log'),
            collect_time('time-scidb-lr-80m-3-1.log'),
        ],
        [
            collect_time('time-numpy-lr-10m-3-1.log'),
            collect_time('time-numpy-lr-20m-3-1.log'),
            collect_time('time-numpy-lr-40m-3-1.log'),
            collect_time('time-numpy-lr-80m-3-1.log'),
        ],
        [
            collect_time('time-dask-lr-10m-3-1.log'),
            collect_time('time-dask-lr-20m-3-1.log'),
            collect_time('time-dask-lr-40m-3-1.log'),
            collect_time('time-dask-lr-80m-3-1.log'),
        ]
    ]

    fig8_dense_nmf_data = [
        [
            collect_time('time-prevision-nmf-10m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-20m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-40m-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-nmf-80m-3-1-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-nmf-10m-3-1.log'),
            collect_time('time-systemds-nmf-20m-3-1.log'),
            collect_time('time-systemds-nmf-40m-3-1.log'),
            collect_time('time-systemds-nmf-80m-3-1.log'),
        ],
        [
            collect_time('time-mllib-nmf-10m-3-1.log'),
            collect_time('time-mllib-nmf-20m-3-1.log'),
            collect_time('time-mllib-nmf-40m-3-1.log'),
            collect_time('time-mllib-nmf-80m-3-1.log'),
        ],
        [
            collect_time('time-madlib-nmf-10m-3-1.log'),
            collect_time('time-madlib-nmf-20m-3-1.log'),
            collect_time('time-madlib-nmf-40m-3-1.log'),
            collect_time('time-madlib-nmf-80m-3-1.log'),
        ],
        [ 
            collect_time('time-scidb-nmf-10m-3-1.log'),
            collect_time('time-scidb-nmf-20m-3-1.log'),
            collect_time('time-scidb-nmf-40m-3-1.log'),
            collect_time('time-scidb-nmf-80m-3-1.log'),
        ],
        [
            collect_time('time-numpy-nmf-10m-3-1.log'),
            collect_time('time-numpy-nmf-20m-3-1.log'),
            collect_time('time-numpy-nmf-40m-3-1.log'),
            collect_time('time-numpy-nmf-80m-3-1.log'),
        ],
        [
            collect_time('time-dask-nmf-10m-3-1.log'),
            collect_time('time-dask-nmf-20m-3-1.log'),
            collect_time('time-dask-nmf-40m-3-1.log'),
            collect_time('time-dask-nmf-80m-3-1.log'),
        ]
    ]

    fig8_sparse_lr_data = [
        [
            collect_time('time-prevision-slr-0.0125-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.025-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.05-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-slr-0.1-3-1-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-slr-0.0125-3-1.log'),
            collect_time('time-systemds-slr-0.025-3-1.log'),
            collect_time('time-systemds-slr-0.05-3-1.log'),
            collect_time('time-systemds-slr-0.1-3-1.log'),
        ],
        [
            collect_time('time-mllib-slr-0.0125-3-1.log'),
            collect_time('time-mllib-slr-0.025-3-1.log'),
            collect_time('time-mllib-slr-0.05-3-1.log'),
            collect_time('time-mllib-slr-0.1-3-1.log'),
        ],
        [
            collect_time('time-madlib-slr-0.0125-3-1.log'),
            collect_time('time-madlib-slr-0.025-3-1.log'),
            collect_time('time-madlib-slr-0.05-3-1.log'),
            collect_time('time-madlib-slr-0.1-3-1.log'),
        ],
        [ 
            collect_time('time-scidb-slr-0.0125-3-1.log'),
            collect_time('time-scidb-slr-0.025-3-1.log'),
            collect_time('time-scidb-slr-0.05-3-1.log'),
            collect_time('time-scidb-slr-0.1-3-1.log'),
        ]
    ]

    fig8_sparse_pr_data = [
        [
            collect_time('time-prevision-pagerank-enron-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-pagerank-epinions-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-pagerank-livejournal-3-1-opt-pe-getpos.log') / 1000000,
            collect_time('time-prevision-pagerank-twitter-3-1-opt-pe-getpos.log') / 1000000,
        ],
        [
            collect_time('time-systemds-pagerank-enron-3-1.log'),
            collect_time('time-systemds-pagerank-epinions-3-1.log'),
            collect_time('time-systemds-pagerank-livejournal-3-1.log'),
            collect_time('time-systemds-pagerank-twitter-3-1.log'),
        ],
        [
            collect_time('time-mllib-pagerank-enron-3-1.log'),
            collect_time('time-mllib-pagerank-epinions-3-1.log'),
            collect_time('time-mllib-pagerank-livejournal-3-1.log'),
            collect_time('time-mllib-pagerank-twitter-3-1.log'),
        ],
        [
            collect_time('time-madlib-pagerank-enron-3-1.log'),
            collect_time('time-madlib-pagerank-epinions-3-1.log'),
            collect_time('time-madlib-pagerank-livejournal-3-1.log'),
            collect_time('time-madlib-pagerank-twitter-3-1.log'),
        ],
        [ 
            collect_time('time-scidb-pagerank-enron-3-1.log'),
            collect_time('time-scidb-pagerank-epinions-3-1.log'),
            collect_time('time-scidb-pagerank-livejournal-3-1.log'),
            collect_time('time-scidb-pagerank-twitter-3-1.log'),
        ]
    ]

    return fig8_dense_lr_data, fig8_dense_nmf_data, fig8_sparse_lr_data, fig8_sparse_pr_data


def render_all_fig8():
    fig8_dense_lr_data, fig8_dense_nmf_data, fig8_sparse_lr_data, fig8_sparse_pr_data = prepare_data()

    mpl.rcParams.update(mpl.rcParamsDefault)

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "12"
    plt.rcParams['ytick.minor.size'] = 0
    plt.rcParams['ytick.minor.width'] = 0

    render_fig8_dense(fig8_dense_lr_data, fig8_dense_nmf_data)
    render_fig8_sparse(fig8_sparse_lr_data, fig8_sparse_pr_data)

if __name__ == "__main__":
    render_all_fig8()
