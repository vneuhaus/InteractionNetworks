import pandas as pd
import numpy as np
from tqdm import tqdm
import math 

def getPipeline(subreddit, start, end, limit=100000):
    pipeline = [
        {'$project': {'_id': 0, 'link_id':0, 'score':0}},#  'id': 1, 'subreddit': 1, 'parent_id': 1, 'author': 1}}, 
        {'$match': {'subreddit': subreddit,'created_utc':{ '$gt': start, '$lt': end },'author': {'$ne': '[deleted]'}}},
        #{'$limit': limit},
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
        cursor = collection.aggregate(getPipeline(str(subreddit),start,end),allowDiskUse=True )
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
    del(df)
    for i,li in tqdm(enumerate(df_array[:,2])): #iterate over posts link id
        pos = np.searchsorted(df_array[:,1],li)
        if pos != df_array.shape[0]:
            if li == df_array[pos,1]:
                df_array[i,4] = df_array[pos,3]
    return df_array

def getData(db,subreddit,start,end,which,save=True):
    try:
        data = np.load('./top100/first1000000/{}_2020.npy'.format(subreddit), allow_pickle=True)
        return data
    except:
        pass
    try:
        df = getDataframe(db,subreddit,start,end,'comments')
        data = getMyParents(df)
        del(df)
        if save:
            np.save('./top100/first100000/{}_2020.npy'.format(subreddit), data)
        return data
    except:
        return None


"""def generateAdjMatrix(data, num_users):
    adj_matrix = np.zeros((num_users, num_users))
    for index, row in df.iterrows():
        if row['link_user_id'] != -1 and not math.isnan(row['link_user_id']) and not math.isnan(row['user_id']):
            adj_matrix[int(row['user_id']),int(row['link_user_id'])] += 1
    adj_matrix = adj_matrix/adj_matrix.max()
    return adj_matrix"""

def getTop(db, top, sortby = None):
    if sortby is None:
        raise NameError('No argument to sort by')
    pipe = [
        {'$sort': {sortby:-1}},
        {'$limit': top},
        {'$project': {'_id':1}}
    ]
    collection = db.subreddit_submissions
    cursor = collection.aggregate(pipe)
    df = pd.DataFrame(list(cursor))
    df_array = df.to_numpy()
    return(df_array)

def getTopNum(db, top, sortby = None):
    if sortby is None:
        raise NameError('No argument to sort by')
    pipe = [
        {'$sort': {sortby:-1}},
        {'$limit': top},
        {'$project': {'comments':1, '_id': -1}}
    ]
    collection = db.subreddit_submissions
    cursor = collection.aggregate(pipe)
    df = pd.DataFrame(list(cursor))
    df_array = df.to_numpy()
    return(df_array)