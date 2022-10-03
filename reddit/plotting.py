import numpy as np
import powerlaw as plw
import seaborn as sns
import matplotlib.pyplot as plt

def plot_degr_distr(graph, ax=None, color='blue',label=None, show_fit=None):
    if ax is None:
        ax = plt.gca()

    degr = np.array(graph.degree())
    degr = degr[degr > 0]
    fit = plw.Fit(degr, discrete=True, xmin=1, xmax=10**4, linear_bins='both')
    fit.plot_pdf(ax=ax, lw=3, color=color, original_data=True, **{'label': label})

    R, p = fit.distribution_compare('lognormal','truncated_power_law',normalized_ratio = True)
    if show_fit == 'both':
        fit.lognormal.plot_pdf(ax=ax, color=color, linestyle='--')
        fit.truncated_power_law.plot_pdf(ax=ax, color=color, linestyle='--')
    elif show_fit == 'best':
        if R > 0:
            print('Lognormal Fit')
            fit.lognormal.plot_pdf(ax=ax, color=color, linestyle='--')
        else:
            print('Truncated Power Law Fit')
            fit.truncated_power_law.plot_pdf(ax=ax, color=color, linestyle='--')

def plot_transitivity_distr(graph, ax=None, color='blue',label=None, show_fit=True):
    if ax is None:
        ax = plt.gca()

    clust = np.array(graph.transitivity_local_undirected())
    clust = clust[clust>0]

    fit = plw.Fit(clust, discrete=False, xmin=0.001, sigma_threshold = .0001, linear_bins=False)
    fit.plot_pdf(ax=ax, lw=3, color=color, original_data=True, **{'label': label})

    if show_fit:
        fit.lognormal.plot_pdf(ax=ax, color=color, linestyle='--')
