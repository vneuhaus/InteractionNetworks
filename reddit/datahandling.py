import pandas as pd
import numpy as np
import tqdm
import math 

def getPipeline(subreddit, start, end):
    pipeline = [
        {'$project': {'_id': 0, 'link_id':0, 'score':0}},#  'id': 1, 'subreddit': 1, 'parent_id': 1, 'author': 1}}, 
        {'$match': {'subreddit': subreddit,'created_utc':{ '$gt': start, '$lt': end }}},#,'author': {'$ne': '[deleted]'}}},
        {'$project': {'subreddit': 0}},
        {'$sort': {'id':1}}
    ]
    return pipeline

def getDataframe(db, subreddit, start, end, which='both'):
    """
	Loads data from given subreddit from database and packs into dataframe
	Args:
		subreddit:
		start:
		end:
        which: 
	Returns:
        Dataframe for given parameters
	"""
    ### Load Database and apply pipeline, assign unique index to each user
    if which != 'both':
        collection = db[which]
        cursor = collection.aggregate(getPipeline(subreddit,start,end))
        #cursor = collection.find({'subreddit':subreddit}).hint('Interactions')
        df = pd.DataFrame(list(cursor))
        df.drop(columns=['subreddit','score', 'link_id','_id','created_utc','num_comments','domain'], errors='ignore', inplace=True)
        if which == 'submissions':
            df.insert(2, 'parent_id', '')

    else:
        df_sub = getDataframe(subreddit, which='submissions')
        df_com = getDataframe(subreddit,which='comments')
        df = pd.concat([df_sub,df_com])
    df['id'] = df['id'].astype('string') 
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t3_','')
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t1_','')
    return df

def getMyParents(df):
    """
	Args:
		df:
	Returns:
        numpy array with parent user id
	"""
    df['user_id'] = df.groupby('author').ngroup()    ##Add user id
    df['parent_user_id'] = pd.Series(dtype=int)
    df = df.assign(parent_user_id = -1)
    df_array = df.to_numpy()
    for i,li in tqdm(enumerate(df_array[:,2])): #iterate over posts link id
        us_id = df_array[:,3][np.array(df_array[:,1])==li]
        if (us_id.size > 0):
            df_array[i,4] = int(us_id)
    return df_array

"""def generateAdjMatrix(data, num_users):
    adj_matrix = np.zeros((num_users, num_users))
    for index, row in df.iterrows():
        if row['link_user_id'] != -1 and not math.isnan(row['link_user_id']) and not math.isnan(row['user_id']):
            adj_matrix[int(row['user_id']),int(row['link_user_id'])] += 1
    adj_matrix = adj_matrix/adj_matrix.max()
    return adj_matrix"""