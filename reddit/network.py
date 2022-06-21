import math
import networkx as nx
import numpy as np

def genDirNet(data, ):
    G = nx.MultiDiGraph()
    num_users = data.shape[0]
    G.add_nodes_from(np.arange(num_users))
    for index, row in enumerate(data):
        if row[4] != -1 and not math.isnan(row[4]) and not math.isnan(row[3]):
            G.add_edge(int(row[3]),row[4])
    return G

def degree_histogram_directed(G, in_degree=False, out_degree=False):
    nodes = G.nodes()
    if in_degree:
        in_degree = dict(G.in_degree())
        degseq=[in_degree.get(k,0) for k in nodes]
    elif out_degree:
        out_degree = dict(G.out_degree())
        degseq=[out_degree.get(k,0) for k in nodes]
    else:
        degseq=[v for k, v in G.degree()]
    dmax=max(degseq)+1
    freq= [ 0 for d in range(dmax) ]
    for d in degseq:
        freq[d] += 1
    return freq