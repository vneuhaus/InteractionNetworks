import pandas as pd
import numpy as np
from tqdm import tqdm
import math 

def getPipeline(subreddit= None, start=None, end=None, limit=400000):
    pipeline = [
        {'$project': {'_id': 0, 'link_id':0, 'score':0}},#  'id': 1, 'subreddit': 1, 'parent_id': 1, 'author': 1}}, 
    ]
    if subreddit is not None:
        pipeline.append({'$match': {'subreddit': subreddit}})
    if start is not None and end is not None:
        pipeline.append({'$match': {'created_utc':{ '$gt': start, '$lt': end }}})
    pipeline.append({'$project': {'subreddit': 0, 'created_utc':0}})    
    return pipeline

def getDataframe(db, subreddit, start=None, end=None, col_name='comments'):
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
        csr = db['comments'].aggregate(getPipeline(str(subreddit),start=start,end=end))
        #csr = db.command('aggregate','comments',getPipeline(str(subreddit),start=start,end=end),{'cursor': { 'batchSize': 0 }})
        df = pd.DataFrame(list(csr))
        #df.drop(columns=['subreddit','score', 'link_id','_id','created_utc','num_comments','domain'], errors='ignore', inplace=True)
        if col_name == 'submissions':
            df.insert(2, 'parent_id', '')
    else:
        df_sub = getDataframe(subreddit,col_name='submissions')
        df_com = getDataframe(subreddit,col_name='comments')
        df = pd.concat([df_sub,df_com])
    #df.drop(df[df.author == '[deleted]'].index)
    df['id'] = df['id'].astype('string') 
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t3_','')
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t1_','')
    df.sort_values(by=['id'], inplace=True)
    df['user_id'] = df.groupby('author').ngroup()    ##Add user id
    df['parent_user_id'] = pd.Series(dtype=int)
    df = df.assign(parent_user_id = -1)
    return df

def getMyParents(df, verbose=False):
    """
	Args:
		df:
	Returns:
        numpy array with parent user id
	"""
    for i,li in (tqdm(enumerate(df['parent_id'])) if verbose else enumerate(df['parent_id'])): #iterate over posts parent id
        pos = df['id'].searchsorted(li)
        if df['author'][pos] == '[deleted]':
                pos = getUndelParentId(df, pos)
        if pos != df.shape[0]:
            if li == df['id'][pos]:
                df.at[i,'parent_user_id'] = df.loc[pos,'user_id']
    return df

def getUndelParentId(df, pos): #if original author is deleted get next user in net
    pos = df['id'].searchsorted(df['parent_id'][pos])
    if pos == df.shape[0]:
        return pos
    if df['author'][pos] == '[deleted]':
        getUndelParentId(df, pos)
    return pos

def getData(db,subreddit,col_name,start=None, end=None, verbose=False):
    if verbose:
        print('Get dataframe from mongodb!')
    df = getDataframe(db,subreddit,start,end,col_name)
    if verbose:
        print('Get parents!')
    data = getMyParents(df, verbose)
    return data

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