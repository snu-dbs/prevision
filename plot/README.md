This README.md describes what script is used for an experiment plot in the paper.
Note that all numbers used for plotting are hard-coded in the scripts.

- Figure 8: `barchart_overall.ipynb`. You may need to make a little modification to produce exactly the same layout.
- Figure 9: `iter.ipynb`.
- Figure 10: `barchart_stack.ipynb`. You may need to make a little modification to produce exactly the same layout.
- Figure 11: `ref_chart/chart.ipynb`
    - You may need to change the input file to generate all outputs. Change the argument of the `open()` in the first jupyter cell. You can choose one among `blocking_lr_fl.log`, `blocking_nmf_fl.log`, `getpos_lr_fl.log`, and `getpos_nmf_fl.log`.
    - You need to make a little modification for each subfigure.
- Figure 13: `barchart_stack.ipynb`. You may need to make a little modification to produce exactly the same layout.
- Figure 14: `ref_chart/fillline.ipynb`.