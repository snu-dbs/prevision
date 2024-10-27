import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

def render_fig9a(nmf_data):
    label = ['TilePACK', 'SystemDS', 'NumPy',
             'Dask', 'SciDB', 'MADlib', 'MLlib']
    x = [1, 2, 4, 8, 16, 32]

    fig = plt.figure(figsize=(5, 3))

    plt.plot(x, np.array(nmf_data[0]), marker='X', color='b')        # PreVision
    plt.plot(x, np.array(nmf_data[1]), marker='s', color='r')        # SystemDS
    plt.plot(x, np.array(nmf_data[2]), marker='o', color='y')        # MLlib
    plt.plot(x, np.array(nmf_data[3]), marker='P', color='m')        # MADlib
    plt.plot(x, np.array(nmf_data[4]), marker='*', color='g')        # SciDB
    plt.plot(x, np.array(nmf_data[5]), marker='^', color='c')        # NumPy
    plt.plot(x, np.array(nmf_data[6]), marker='d', color='k')        # Dask

    plt.xlim(1, 32)
    plt.ylim(10, 200000)
    plt.yscale('log')
    plt.tick_params(axis='both', which='major', labelsize=18)
    plt.ylabel("Elapsed Time (s)", fontdict={'size': 18})

    plt.xlabel("The Number of Iterations", fontdict={'size': 18}, labelpad=12)
    plt.grid()

    fig.patch.set_facecolor('white')

    plt.savefig('output/fig9_a.pdf', format='pdf', bbox_inches='tight')


def render_fig9_legend():
    x = [1]

    fig = plt.figure(figsize=(4, 2))
    ax = fig.add_axes([0, 0, 1, 1])

    plt.plot(x, np.array([0]), marker='X', color='b', label='PreVision')
    plt.plot(x, np.array([0]), marker='s', color='r', label='SystemDS')
    plt.plot(x, np.array([0]), marker='o', color='y', label='MLlib')
    plt.plot(x, np.array([0]), marker='P', color='m', label='MADlib')
    plt.plot(x, np.array([0]), marker='*', color='g', label='SciDB')
    plt.plot(x, np.array([0]), marker='^', color='c', label='NumPy')
    plt.plot(x, np.array([0]), marker='d', color='k', label='Dask')

    ax.tick_params(axis='both', which='major', labelsize=17)
    ax.grid()

    lfig = plt.figure(figsize=(4.5, 2.2))
    plt.figlegend(*ax.get_legend_handles_labels(), loc='upper center',
                  ncol=4, mode="expand", fontsize=11, numpoints=1)
    lfig.savefig('output/fig9_legend.pdf', format='pdf', bbox_inches='tight')


def render_fig9b(pr_data):
    x = [1, 2, 4, 8, 16, 32]
    fig = plt.figure(figsize=(5, 3))

    plt.plot(x, np.array(pr_data[0]), marker='X', color='b')    # PreVision
    plt.plot(x, np.array(pr_data[1]), marker='o', color='y')              # MLlib
    plt.plot([1, 2, 4, 8], np.array(pr_data[2]), marker='P', color='m')   # MADlib
    plt.plot(x, np.array(pr_data[3]), marker='*', color='g')              # SciDB

    plt.xlim(1, 32)
    plt.ylim(10, 200000)
    plt.yscale('log')
    plt.tick_params(axis='both', which='major', labelsize=18)

    plt.xlabel("The Number of Iterations", fontdict={'size': 18}, labelpad=12)

    fig.patch.set_facecolor('white')

    plt.grid()
    plt.savefig('output/fig9_b.pdf', format='pdf', bbox_inches='tight')


def render_all_fig9():
    from data import fig9_nmf_data, fig9_pr_data

    mpl.rcParams.update(mpl.rcParamsDefault)

    plt.style.use('classic')

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "14"

    render_fig9a(fig9_nmf_data)
    render_fig9b(fig9_pr_data)
    render_fig9_legend()


if __name__ == '__main__':
    render_all_fig9()