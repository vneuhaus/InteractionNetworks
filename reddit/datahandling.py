import pandas as pd
import numpy as np
from tqdm.notebook import tqdm

def save_to_hdf5(db, collection, file, subreddit):
    """
    Loads data from the database for a single subreddit and saves it in an hdf5 file
    Args:
        db:
        collection:
        file:
        subreddit:
    Returns:
    """
    csr = db[collection].find({'subreddit': subreddit},{'_id': 0, 'link_id':0, 'score':0, 'subreddit': 0, 'created_utc':0}, batch_size=100000)
    ls = []
    i = 0
    for doc in tqdm(csr):
        if i == 10000:
            df = (pd.DataFrame(ls))
            df.to_hdf(file,key='df',min_itemsize=18,append=True)
            ls = []
            i = 0
        i += 1
        ls.append(doc)
    df = (pd.DataFrame(ls))
    df.to_hdf(file,key='df',min_itemsize=18,append=True)

def preprocess_hdf(file_from, file_to, rem_bots=True, rem_del=True):
    """
    Preprocesses the data saved in an hdf5 file and saves in a new one
    Args:
        file_from:
        file_to:
        rem_bots: if true removes 
        rem_del: if true removes posts with deleted authors; otherwise those posts are also removed but users connections are preserved
    Returns:
    """
    df = pd.read_hdf(file_from, key='df')
    bots = np.load('bots.npy')
    
    if rem_bots: ##Remove posts created by bots
        active_bots = np.intersect1d(np.unique(df['author']), bots)
        df.reset_index(inplace=True, drop=True) ###Generate correct index
        df = df[~df.author.isin(active_bots)] ###Removes Bots that are listed on https://botrank.pastimes.eu/

    if rem_del: ##Remove posts created by deleted users, if not "reconnect" users
        df.reset_index(inplace=True, drop=True) ###Generate correct index
        df = df[~df.author.isin(['[deleted]'])] 

    df.reset_index(inplace=True, drop=True) ###Generate correct index

    df['id'] = df['id'].astype('string') 
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t3_','')
    df['parent_id'] = df['parent_id'].astype('string').str.replace('t1_','')
    df.sort_values(by=['id'], inplace=True)

    ### Add parent author to the dataframe
    df['user_id'] = df.groupby('author').ngroup()    ##Add user id
    for i,li in (tqdm(enumerate(df['parent_id']), total=len(df.index)) if True else enumerate(df['parent_id'])): #iterate over posts parent id
        pos = df['id'].searchsorted(li)
        if pos != df.shape[0]:
            if df['author'][pos] == '[deleted]':
                    pos = getUndelParentId(df, pos, 0)
            if pos != df.shape[0]:
                if li == df['id'][pos]: 
                    df.at[i,'parent_author'] = df.loc[pos,'author']

    df[['author','id','parent_id', 'parent_author']] = df[['author','id','parent_id', 'parent_author']].astype(object)
    df[['user_id']] = df[['user_id']].astype(int)
    
    df.to_hdf(file_to, key='df')


def getUndelParentId(df, pos, n=0): #if original author is deleted get next user in net
    if pos == df.shape[0] or n == 3:
        return df.shape[0]
    else:
        pos = df['id'].searchsorted(df['parent_id'][pos])
    if pos != df.shape[0]:
        if df['author'][pos] == '[deleted]':
            getUndelParentId(df, pos, n+1)
    return pos