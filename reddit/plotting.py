import numpy as np
import powerlaw as plw
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import linregress

def plotpowerlaw(data, ax=None, show_fit=True, color='blue', xmin=1):
    """Plots the probability distribution of data with a power-law fit
	Args:
		data (list): 
		ax (None, optional): 
		show_fit (bool, optional): 
		xmin (int, optional): 
	"""
    if ax is None:
        ax = plt.gca()
    # Plots data
    data_nonzero = data[data > 0]
    pl_obj = plw.Fit(data_nonzero, discrete=True, xmin=xmin)
    str_label = r'N = {:0.0f}'.format(data_nonzero.size)
    pl_obj.plot_pdf(ax=ax, original_data=True,color=color, **{'label': str_label})
    if show_fit:
        str_label_fit = r'$\alpha$ = {:0.3f}'.format(pl_obj.power_law.alpha)
        pl_obj.power_law.plot_pdf(ax=ax, color=color, linestyle='--', **{'label': str_label_fit})
    ax.legend()

def plotdegrees(data_in, data_out, ax=None, show_lin=True, subreddit=None, color='blue'):
    if ax is None:
        ax = plt.gca()
    if show_lin:
        xmax = data_in.max()
        ax.plot([0,xmax],[0,xmax],ls='--',lw=2,color='black')
    if subreddit is not None:
        ax.set_title(subreddit)
    slope, intercept, r, p, se = linregress(data_in, data_out,alternative='greater')
    str_label = r'r-Value: {:0.3f}'.format(r)
    ax.scatter(data_in, data_out, color=color, alpha=0.3, **{'label': str_label})
    ax.legend()
    ax.set_box_aspect(1)