import numpy as np
import powerlaw as plw
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import linregress

def plotpowerlaw(data, ax=None, show_fit=True, discrete=True, color='blue', xmin=1):
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
    fit = plw.Fit(data_nonzero, discrete=discrete, xmin=xmin, linear_bins=True)
    #str_label = r'N = {:0.0f}'.format(data_nonzero.size)
    fit.plot_pdf(ax=ax, original_data=True)#, **{'label': str_label})

    if show_fit:
        str_label_fit = r'$\alpha$ = {:0.3f}'.format(fit.power_law.alpha)
        fit.power_law.plot_pdf(ax=ax, color='k', linestyle='--', **{'label': str_label_fit})
    ax.legend()
    return fit.power_law.alpha

def plotlognormal(data, ax=None, show_fit=True, discrete=True, color='blue', xmin=1):
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
    fit = plw.Fit(data_nonzero, discrete=discrete, xmin=xmin, xmax=None, discrete_approximation='round')
    #str_label = r'N = {:0.0f}'.format(data_nonzero.size)
    fit.plot_pdf(ax=ax, original_data=True,color=color)#, **{'label': str_label})

    if show_fit:
        str_label_fit = r'$\mu$ = {:0.3f}, $\sigma$ = {:0.3f}'.format(fit.lognormal_positive.mu, fit.lognormal_positive.sigma)
        fit.lognormal_positive.plot_pdf(ax=ax, color='k', linestyle='--', **{'label': str_label_fit})
    ax.legend()

def plotDegreedistr(G, axis, in_degree=False, out_degree=False, color='blue'):
    if in_degree:
        degr = np.array(G.in_degree())[:,1]
    elif out_degree:
        degr = np.array(G.out_degree())[:,1]
    else:
        degr = np.array(G.degree())[:,1]
    #alpha = plotting.plotpowerlaw(degr, axis, show_fit=True, discrete=False, xmin=1)
    plotlognormal(degr, axis, show_fit=True, color=color, discrete=False, xmin=1)
    return 0

def plotdegrees(data_in, data_out, ax=None, show_lin=True, title=None, color='blue'):
    if ax is None:
        ax = plt.gca()
    if show_lin:
        xmax = data_in.max()
        ax.plot([0,xmax],[0,xmax],ls='--',lw=2,color='black', label='x=y')
    
    slope, intercept, r, p, se = linregress(data_in, data_out,alternative='greater')
    str_label = r'r-Value: {:0.3f}'.format(r)
    ax.scatter(data_in, data_out, color=color, alpha=0.3, **{'label': str_label})
    ax.legend()
    ax.set_title(title)
    ax.set_box_aspect(1)