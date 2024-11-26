import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from util import collect_breakdown

def sb(ax, theme, label, stacktype, data, title, ylabel=None):
    width = 0.64
    bars = None
    bottoms = np.zeros(len(label))
    for idx, item in enumerate(data):
        print(idx, item)
        bar = ax.bar(label, item, width=width, bottom=bottoms, color=theme[idx][0], hatch=theme[idx][1], edgecolor='black', label=stacktype[idx])
        bottoms += item
        bars = bars + bar if bars is not None else bar
    
    ax.set_xlim([-0.6, 2.6])
    if ylabel is not None:
        ax.set_ylabel(ylabel, fontsize=14)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color='#CCCCCC')
    ax.set_ylim(bottom=0, top=max(data.sum(axis=0)) * 1.1)

def render_fig14(lr_data, nmf_data):
    theme = [
        ['black', ''],
        ['white', '||||'],
        ['white', '\\\\\\\\'],
        ['white', ''],
        ['white', '////'],
        ['white', 'xxxx'],
        # ['white', '++++'],
    ]
    label = ['OPT', 'MRU', 'LRU-2', ]
    stacktype = ["I/O", "List Maintenance", "Query Planning", "CPU", ]

    lr_data = np.array(lr_data).T / 1000000
    nmf_data = np.array(nmf_data).T / 1000000

    fig = plt.figure(figsize=(2, 2.2))
    ax = fig.add_axes([0, 0, 1, 1])
    sb(ax, theme, label, stacktype, lr_data, "LR", ylabel="Elapsed Time (s)")
    ax.set_ylim(0, 400)
    plt.savefig('output/fig14_a.pdf', format='pdf', bbox_inches='tight')

    fig = plt.figure(figsize=(2, 2.2))
    ax = fig.add_axes([0, 0, 1, 1])
    sb(ax, theme, label, stacktype, nmf_data, "NMF")
    ax.set_ylim(0, 800)
    plt.savefig('output/fig14_b.pdf', format='pdf', bbox_inches='tight')

    lfig = plt.figure(figsize=(5, 2.2))
    print(ax.get_legend_handles_labels())
    plt.figlegend(*ax.get_legend_handles_labels(), loc = 'upper center', ncol=5, mode="expand", fontsize=11)
    lfig.savefig('output/fig14_legend.pdf', format='pdf', bbox_inches='tight')


def prepare_data():
    fig14_lr_data = [
	list(collect_breakdown('breakdown-prevision-lr-80m-3-1-opt-pe-getpos.log')),
	list(collect_breakdown('breakdown-prevision-lr-80m-3-1-mru-pe-getpos.log')),
	list(collect_breakdown('breakdown-prevision-lr-80m-3-1-lruk-pe-getpos.log')),
    ]

    fig14_nmf_data = [
	list(collect_breakdown('breakdown-prevision-nmf-80m-3-1-opt-pe-getpos.log')),
	list(collect_breakdown('breakdown-prevision-nmf-80m-3-1-mru-pe-getpos.log')),
	list(collect_breakdown('breakdown-prevision-nmf-80m-3-1-lruk-pe-getpos.log')),
    ]

    return fig14_lr_data, fig14_nmf_data


def render_all_fig14():
    fig14_lr_data, fig14_nmf_data = prepare_data()

    plt.rcParams.update(plt.rcParamsDefault)

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = "12"

    render_fig14(fig14_lr_data, fig14_nmf_data)


if __name__ == '__main__':
    render_all_fig14()
