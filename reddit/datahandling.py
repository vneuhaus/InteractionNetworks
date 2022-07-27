import pandas as pd
import numpy as np
from tqdm import tqdm
import math 

def getPipeline(subreddit, limit=400000):
    pipeline = [
        {'$project': {'_id': 0, 'link_id':0, 'score':0, 'created_utc':0,}},#  'id': 1, 'subreddit': 1, 'parent_id': 1, 'author': 1}}, 
        {'$match': {'subreddit': subreddit}},
        {'$project': {'subreddit': 0}},
        #{'$match': {'author': {'$ne': '[deleted]'}}},
        #{'$sort': {'id':1}},
    ]
    return pipeline
#,'created_utc':{ '$gt': start, '$lt': end }
def getDataframe(db, subreddit, col_name='both'):
    """
	Loads data from given subreddit from database and packs into dataframe
	Args:
		subreddit:
		start:
		end:
        which: Defines the collection that shall be loaded
	Returns:
        Dataframe for given parameters
	"""
    ### Load Database and apply pipeline, assign unique index to each user
    if col_name != 'both':
        collection = db[col_name]
        cursor = collection.aggregate(getPipeline(str(subreddit)),allowDiskUse=True)
        #cursor = collection.find({'subreddit':subreddit}).hint('Interactions')
        df = pd.DataFrame(list(cursor))
        df.drop(columns=['subreddit','score', 'link_id','_id','created_utc','num_comments','domain'], errors='ignore', inplace=True)
        if col_name == 'submissions':
            df.insert(2, 'parent_id', '')
    else:
        df_sub = getDataframe(subreddit, which='submissions')
        df_com = getDataframe(subreddit,which='comments')
        df = pd.concat([df_sub,df_com])
    df['id'] = df['id'].astype('string') 
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t3_','')
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t1_','')
    return df

def getMyParents(df, verbose=False):
    """
	Args:
		df:
	Returns:
        numpy array with parent user id
	"""
    print('Sorting Dataframe...')
    df.drop(df[df.author == '[deleted]'].index)
    df.sort_values(by=['id'], inplace=True)
    df['user_id'] = df.groupby('author').ngroup()    ##Add user id
    df['parent_user_id'] = pd.Series(dtype=int)
    df = df.assign(parent_user_id = -1)
    df_array = df.to_numpy()
    del(df)
    for i,li in (tqdm(enumerate(df_array[:,2])) if verbose else enumerate(df_array[:,2])): #iterate over posts link id
        pos = np.searchsorted(df_array[:,1],li)
        if pos != df_array.shape[0]:
            if li == df_array[pos,1]:
                df_array[i,4] = df_array[pos,3]
    return df_array

def getData(db,subreddit,col_name,save=True, verbose=False):
    if verbose:
        print('Get dataframe from mongodb!')
    df = getDataframe(db,subreddit,col_name)
    if verbose:
        print('Get parents!')
    data = getMyParents(df, verbose)
    del(df)
    if save:
        np.save('./top100/first100000/{}_2020.npy'.format(subreddit), data)
    return data


"""def generateAdjMatrixs(data, num_users):
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