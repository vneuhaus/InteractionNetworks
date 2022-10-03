import pandas as pd
from tqdm.notebook import tqdm
from itertools import combinations
import igraph as ig

def hdf_to_net(file,directed):
    """
	Loads data from the hdf5 files and generates user interaction network
	Args:
		file: location of the hdf5 file
		directed: 
	Returns: User interaction graph
	"""
    df = pd.read_hdf(file,key = 'df')
    G = ig.Graph.DataFrame(df[['author','parent_author']].astype(str), directed)
    return G

def get_triangles(G):
    """
	Calculates the number of triangles for each node (can be extremely slow!)
	Args:
		G: 
	Returns:
	"""
    result = [0] * G.vcount()
    adjlist = [set(neis) for neis in G.get_adjlist()]
    for vertex, neis in enumerate(adjlist):
        for nei1, nei2 in combinations(neis, 2):
            if nei1 in adjlist[nei2]:
                result[vertex] += 1
    return result