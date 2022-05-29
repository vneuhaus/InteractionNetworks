from pymongo import MongoClient
from sys import getsizeof
import numpy as np 
import pandas as pd
import collections
import matplotlib.pyplot as plt
import matplotlib
import networkx as nx
from sympy import degree
from tqdm import tqdm

start_time = 1577836800
end_time = 1585699200
subreddit = 'desmoines'

com_pipeline = [
    {
        '$match': {
        	'created_utc': {
                '$gte': start_time, 
                '$lt': end_time
            },
        	'subreddit': subreddit, 
            'author': {
                '$ne': '[deleted]'
            }
        }
    }
]

sub_pipeline = [
    {
        '$match': {
            'created_utc': {
                '$gte': start_time, 
                '$lt': end_time
            },
            'subreddit': subreddit,
            'author': {
                '$ne': '[deleted]'
            },
            'num_comments': {
                '$ne': 0
            }
        }
    }
]

#networkx graph design
options = {
    #'node_color': 'red',
    'node_size': 1,
    'width': 0.2,
    'arrowsize': 2,
    #'arrowstyle': 'fancy',
}

client = MongoClient('localhost', 27017)
db = client.reddit


### Load Database and apply pipeline, assign unique index to each user
com_collection = db["comments"]
com_cursor = com_collection.aggregate(com_pipeline)
com_df = pd.DataFrame(list(com_cursor))

com_df = com_df.drop(columns=['subreddit','score', 'parent_id', '_id'])

sub_collection = db['submissions']
sub_cursor = sub_collection.aggregate(sub_pipeline)
sub_df = pd.DataFrame(list(sub_cursor))

sub_df = sub_df.drop(columns=['subreddit','score','num_comments','domain', '_id'])
sub_df.insert(2, 'link_id', '')

df = pd.concat([sub_df,com_df])
df['user_id'] = df.groupby('author').ngroup()

df['link_id'] = df['link_id'].astype('string').str.replace('t3_','')
#df['link_id'] = df['link_id']
df['id'] = df['id'].astype('string') 

num_users = df['user_id'].max()+1

df['link_user_id'] = pd.Series(dtype=int)
df = df.assign(link_user_id = -1)
df = df.sort_values('created_utc')


for i,li in tqdm(enumerate(df['link_id'])): #i in tqdm(range(len(com_df.index))): #
	#temp_df = df[df.created_utc < df['created_utc'].iloc[i]]
	link_user_id =  (df.loc[df['id'] == li, 'user_id'])
	if li != '' and not link_user_id.empty:
		df.at[i,'link_user_id'] = link_user_id.item()
df['link_user_id'] = df['link_user_id'].astype('int') 

G = nx.Graph()
G.add_nodes_from(np.linspace(0,num_users+1,num_users+2))

adj_matrix = np.array((num_users, num_users))
for index, row in tqdm(df.iterrows()):
    if row['link_user_id'] != -1:
        adj_matrix[row['user_id']][row['link_user_id']] += 1
        #G.add_edge(row['user_id'],row['link_user_id'])
        
adj_matrix = adj_matrix/adj_matrix.max()

for i in range(num_users):
    for j in range(num_users):
        G.add_edge(i,j, weight=adj_matrix[i,j])


print(G.number_of_edges())
subax1 = plt.subplot()
G.remove_nodes_from(list(nx.isolates(G)))
print(sorted(G.degree, key=lambda x: x[1], reverse=True))


ego = nx.ego_graph(G, n=79,radius=1.5)
color_map = ['red' if node == 79 else 'blue' for node in ego] 
nx.draw_networkx(ego, with_labels=False, node_color=color_map, font_weight='bold',**options)
plt.show()
#deg_inc = 
#deg_out = 
#deg_hist = degree_histogram(G)
deg = G.degree
print(sum(nx.triangles(G).values())/3)

nx.draw_circular(G, with_labels=False, font_weight='bold',**options)
plt.savefig('desmoines_2020_graph.pdf')
plt.show()
