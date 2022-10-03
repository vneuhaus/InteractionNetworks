import math
import networkx as nx
import numpy as np
import pandas as pd
from tqdm.notebook import tqdm
from itertools import combinations
import igraph as ig

def hdf_to_net(file,directed):
    df = pd.read_hdf(file,key = 'df')
    G = ig.Graph.DataFrame(df[['author','parent_author']].astype(str), directed)
    return G


def genNewNet(edges):
    G = nx.MultiDiGraph()
    num_users = np.unique(edges[0]).shape[0]
    edges = edges[:,np.array(edges[1])!=-1]
    G.add_nodes_from(np.arange(num_users))
    for edge in np.transpose(edges):
        G.add_edge(int(edge[0]),edge[1])
    return G

def degree_histogram_directed(G, mode='all', normalize=False):
    nodes = G.nodes()
    degr = dict(G.degree(mode=mode))
    degseq=[degr.get(k,0) for k in nodes]
    dmax=max(degseq)+1
    freq= [ 0 for d in range(dmax) ]
    for d in degseq:
        freq[d] += 1
    if normalize:
        freq = np.divide(freq, np.max(freq))
    return freq

def get_triangles(G):
    result = [0] * G.vcount()
    adjlist = [set(neis) for neis in G.get_adjlist()]
    for vertex, neis in enumerate(adjlist):
        if (len(neis)) < 10000:
            for nei1, nei2 in combinations(neis, 2):
                if nei1 in adjlist[nei2]:
                    result[vertex] += 1
    return result