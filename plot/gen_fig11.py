import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from util import collect_io

def sb(ax, theme, label, stacktype, data, title, ylabel=None):
    width = 0.62
    bars = None
    bottoms = np.zeros(len(label))
    for idx, item in enumerate(data):
        print(idx, item)
        bar = ax.bar(label, item, width=width, bottom=bottoms, color=theme[idx][0], hatch=theme[idx][1], edgecolor='black', label=stacktype[idx])
        bottoms += item
        bars = bars + bar if bars is not None else bar
    
    ax.set_xlim([-0.6, 3.6])
    if ylabel is not None:
        ax.set_ylabel(ylabel, fontsize=14)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color='#CCCCCC')
    ax.set_ylim(bottom=0, top=max(data.sum(axis=0)) * 1.1)

def render_fig11(lr_data, nmf_data):
    theme = [
        ['black', ''],
        ['white', ''],
    ]
    label = ['getPos \nw/ PE', 'getPos \nw/o PE', 'blocking \nw/ PE', 'blocking \nw/o PE']
    stacktype = ["Read", "Write", ]

    lr_data = np.array(lr_data).T / 1000000000
    nmf_data = np.array(nmf_data).T / 1000000000

    fig = plt.figure(figsize=(2.4, 2.4))
    ax = fig.add_axes([0, 0, 1, 1])
    sb(ax, theme, label, stacktype, lr_data, "LR", ylabel="I/O Volume (GB)")
    ax.tick_params(labelsize=11)
    plt.savefig('output/fig11_a.pdf', format='pdf', bbox_inches='tight')

    fig = plt.figure(figsize=(2.4, 2.4))
    ax2 = fig.add_axes([0, 0, 1, 1])
    sb(ax2, theme, label, stacktype, nmf_data, "NMF")
    ax2.tick_params(labelsize=11)
    plt.savefig('output/fig11_b.pdf', format='pdf', bbox_inches='tight')

    lfig = plt.figure(figsize=(2.4, 0.6))
    plt.figlegend(*ax.get_legend_handles_labels(), loc = 'upper center', ncol=5, mode="expand", fontsize=11)
    lfig.savefig('output/fig11_legend.pdf', format='pdf', bbox_inches='tight')


def prepare_data():
    fig11_lr_data = [
	list(collect_io('io-prevision-lr-80m-3-1.log')),
	list(collect_io('io-prevision_wo_pe-lr-80m-3-1.log')),
	list(collect_io('io-prevision_blocking-lr-80m-3-1.log')),
	list(collect_io('io-prevision_blocking_wo_pe-lr-80m-3-1.log'))
    ]

    fig11_nmf_data = [
	list(collect_io('io-prevision-nmf-80m-3-1.log')),
	list(collect_io('io-prevision_wo_pe-nmf-80m-3-1.log')),
	list(collect_io('io-prevision_blocking-nmf-80m-3-1.log')),
	list(collect_io('io-prevision_blocking_wo_pe-nmf-80m-3-1.log'))
    ]

    return fig11_lr_data, fig11_nmf_data


def render_all_fig11():
    fig11_lr_data, fig11_nmf_data = prepare_data()

    plt.rcParams.update(plt.rcParamsDefault)

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "12"

    render_fig11(fig11_lr_data, fig11_nmf_data)


if __name__ == '__main__':
    render_all_fig11()
