import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from util import collect_time, collect_breakdown

def render_fig15_a(prevision_data, numpy_data):
    x = ['100', '200', '400', '800', '1600', '3200']

    total = np.array(prevision_data[0]) / 1000000
    overhead = np.array(prevision_data[1]) / 1000000

    second = np.array([numpy_data for _ in range(6)]) / 1000000

    plt.figure(figsize=(4, 2))

    plt.fill_between(x, 0, overhead, color='gray', edgecolor='black')
    plt.fill_between(x, overhead, total, color='lightgray', edgecolor='gray')
    plt.plot(x, second, color='black')

    plt.plot(x, overhead, color='black')
    plt.plot(x, total, color='gray', marker='x')

    plt.xlim(0, 5)
    plt.ylim(0, 1000)
    plt.tick_params(axis='both', which='major', labelsize=12)
    plt.ylabel("Elapsed Time (s)", fontdict={'size': 12})

    plt.xlabel("The Number of Tiles (Tile Size)", fontdict={'size': 12}, labelpad=20)

    plt.text(3.8, 850, "NumPy", {'size': 14, 'weight': 'bold'})

    plt.text(-0.4, -220, "(640MB)", {'size': 10})
    plt.text(0.6, -220, "(320MB)", {'size': 10})
    plt.text(1.6, -220, "(160MB)", {'size': 10})
    plt.text(2.66, -220, "(80MB)", {'size': 10})
    plt.text(3.66, -220, "(40MB)", {'size': 10})
    plt.text(4.66, -220, "(20MB)", {'size': 10})

    plt.grid()

    plt.legend(['Overhead', 'Total'], prop={'size': 10}, loc='upper left')

    plt.annotate('Overhead', 
        ha='center', va='bottom', weight='bold', fontsize=14,
        xytext=(3.8, 160), xy=(4.8, 30), 
        arrowprops={'edgecolor': '#b90016', 'facecolor': '#b90016', 'shrink': 0.05, 'width': 2, 'headwidth': 8, 'headlength': 8})

    plt.yticks([0, 200, 400, 600, 800, 1000])

    plt.savefig('output/fig15_a.pdf', format='pdf', bbox_inches='tight')

def render_fig15_b(prevision_data, dask_data):
    x = ['100', '200', '400', '800', '1600', '3200']

    total = np.array(prevision_data[0]) / 1000000
    overhead = np.array(prevision_data[1]) / 1000000

    second = np.array(dask_data) / 1000000

    plt.figure(figsize=(4, 2))

    plt.fill_between(x, 0, overhead, color='gray', edgecolor='black')
    plt.fill_between(x, overhead, total, color='lightgray', edgecolor='gray')
    plt.plot(x, second, color='black', marker='+')

    plt.plot(x, overhead, color='black')
    plt.plot(x, total, color='gray', marker='x')


    plt.xlim(0, 5)
    plt.tick_params(axis='both', which='major', labelsize=12)

    plt.xlabel("The Number of Tiles (Tile Size)", fontdict={'size': 12}, labelpad=20)

    plt.text(3.02, 3300, "Dask", {'size': 14, 'weight': 'bold'})

    plt.text(-0.4, -1100, "(640MB)", {'size': 10})
    plt.text(0.6, -1100, "(320MB)", {'size': 10})
    plt.text(1.6, -1100, "(160MB)", {'size': 10})
    plt.text(2.65, -1100, "(80MB)", {'size': 10})
    plt.text(3.65, -1100, "(40MB)", {'size': 10})
    plt.text(4.65, -1100, "(20MB)", {'size': 10})

    plt.annotate('Overhead', 
        ha='center', va='bottom', weight='bold', fontsize=14,
        xytext=(3.8, 1200), xy=(4.8, 100), 
        arrowprops={'edgecolor': '#b90016', 'facecolor': '#b90016', 'shrink': 0.05, 'width': 2, 'headwidth': 8, 'headlength': 8})

    plt.ylim(0, 5000)
    plt.yticks([0, 1000, 2000, 3000, 4000, 5000])

    plt.grid()

    plt.savefig('output/fig15_b.pdf', format='pdf', bbox_inches='tight')


def prepare_data():
    lr_prevision_raw = [
        collect_breakdown('breakdown-prevision-lr-80m-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-lr-80m_200x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-lr-80m_400x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-lr-80m_800x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-lr-80m_1600x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-lr-80m_3200x1-3-1-opt-pe-getpos.log')
    ]

    nmf_prevision_raw = [
        collect_breakdown('breakdown-prevision-nmf-80m-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-nmf-80m_200x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-nmf-80m_400x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-nmf-80m_800x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-nmf-80m_1600x1-3-1-opt-pe-getpos.log'),
        collect_breakdown('breakdown-prevision-nmf-80m_3200x1-3-1-opt-pe-getpos.log')
    ]

    fig15_lr_prevision_data = [[], []]
    fig15_nmf_prevision_data = [[], []]

    for t in lr_prevision_raw:
        fig15_lr_prevision_data[0].append(t[0] + t[1] + t[2] + t[3])
        fig15_lr_prevision_data[1].append(t[2] + t[3])
    
    for t in nmf_prevision_raw:
        fig15_nmf_prevision_data[0].append(t[0] + t[1] + t[2] + t[3])
        fig15_nmf_prevision_data[1].append(t[2] + t[3])
    

    fig15_lr_numpy_data = collect_time('time-numpy-lr-80m-3-1.log') * 1000000
    fig15_nmf_dask_data = [
        collect_time('time-dask-nmf-80m-3-1.log') * 1000000,
        collect_time('time-dask-nmf-80m_200x1-3-1.log') * 1000000,
        collect_time('time-dask-nmf-80m_400x1-3-1.log') * 1000000,
        collect_time('time-dask-nmf-80m_800x1-3-1.log') * 1000000,
        collect_time('time-dask-nmf-80m_1600x1-3-1.log') * 1000000,
        collect_time('time-dask-nmf-80m_3200x1-3-1.log') * 1000000
    ]

    return fig15_lr_prevision_data, fig15_lr_numpy_data, fig15_nmf_prevision_data, fig15_nmf_dask_data

def render_all_fig15():
    fig15_lr_prevision_data, fig15_lr_numpy_data, fig15_nmf_prevision_data, fig15_nmf_dask_data = prepare_data()
    
    mpl.rcParams.update(mpl.rcParamsDefault)

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "14"

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42

    render_fig15_a(fig15_lr_prevision_data, fig15_lr_numpy_data)
    render_fig15_b(fig15_nmf_prevision_data, fig15_nmf_dask_data)

if __name__ == '__main__':
    render_all_fig15()
