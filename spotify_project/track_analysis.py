#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 23:25:09 2019

@author: yanyanyu
"""

import os
import boto3
import json
import datetime as dt
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from spotify_app import open_json,MySpotify
from popularity import Popularity

from sklearn.linear_model import LinearRegression
import statsmodels.api as sm


class UploadAnalysisS3(MySpotify):
    """Upload analysis of all tracks of all the artists in one genre on local machine to AWS S3.
       No need to run this class everyday
       
       Generate analysis using MySpotify from spotify_app.py before uploading.
       
   """
       
       
    def __init__(self,bucket_name='kpopanalysis',genre='k-pop'):
        super().__init__(genre)
        self.s3= boto3.client('s3',region_name='us-east-1')
        self.bucket_name=bucket_name
        self.create_bucket()
    
    def create_bucket(self):
        """Create bucket on S3 if not existed"""
        
        bucket=self.s3.list_buckets()['Buckets']
        have=False
        for i in bucket:            
            if i['Name']==self.bucket_name:
                have=True
        if have==False:
            self.s3.create_bucket(Bucket=self.bucket_name)
    
    
    def upload_s3(self):
        
        list_d=os.listdir('./data/analysis/')
        obj=self.s3.list_objects_v2(Bucket=self.bucket_name)
        if 'Contents' not in obj.keys():
            files=[]
        else:
            Content=obj['Contents']        
            files=[i['Key'] for i in Content]
        
        for i in list_d:
            if i not in files:
                self.s3.upload_file('./data/analysis/'+i, self.bucket_name,i)
                print("Upload {} to S3".format(i))



class Analyze(object):   
    
    def __init__(self,genre='k-pop'):

        self.features=['acousticness','liveness',          
                  'instrumentalness','speechiness', 
                   'danceability','energy','valence','tempo']
        self.poplr=Popularity()
    
    def extract_data(self,artist):
        df=pd.read_hdf('./data/analysis/{}_tracks_analysis.h5'.format(artist))
        return df
 
    
    def extract_data_all(self):
        list_d=os.listdir('./data/analysis/')
        artists=[i[:-19] for i in list_d]
        
        df_all=pd.DataFrame()
        for i in artists:
            df_all=df_all.append(self.extract_data(i))
        
        
        
        return df_all
    
    
    def transform_data(self,df):    
        df=df[df.release_date.str.len()==10]
        df['year']=pd.DatetimeIndex(df.release_date).year
        df['month']=pd.DatetimeIndex(df.release_date).month
        df=df[df.year>=1990]
        
        #get all artists popularity
        artists=self.poplr.query_popularity()
        top_aritsts=artists.sort_values('popularity',ascending=False).reset_index()
        top_aritsts=top_aritsts.rename(columns={'index':'artist'})

        #merge popularity info to the large DataFrame
        df_top=df.merge(top_aritsts,on='artist',how='inner')
        df_top.popularity=df_top.popularity.astype(float)

        
        return df_top
        
    def plot_hist(self,df,artist):
        fig,ax=plt.subplots(2,4,figsize=(12,6), constrained_layout=True)
        num=0
        for i in range(0,2):
            for j in range(0,4):
                ax[i,j].hist(df[self.features[num]],bins=50)
                ax[i,j].set_xlabel(self.features[num])
                num+=1
        fig.suptitle("Histogram of Features Extracted from {}'s Tracks".format(artist))
        plt.tight_layout()
        fig.subplots_adjust(top=0.92)

        
    def feature_analysis(self):
        
        #extract every artists analysis, transform, and append together
        df=self.extract_data_all()
        
        df_top=self.transform_data(df)
            
        #rank popularity
        df_top['rank']=df_top.followers.rank(method='dense',ascending=False) 
        
        #only use the most recent 2 years of data, this will exclude non-active artists
        df_top=df_top[df_top.year>=2017]
                
        #get mean of features of every artist
        mean_feature=pd.DataFrame(columns=['artist'])
        for fea in self.features:
            temp=df_top.groupby('artist')[fea].apply(lambda x:x.clip(x.quantile(0.1),x.quantile(0.9)).mean()).reset_index()
            mean_feature=mean_feature.merge(temp,on='artist',how='right')
        mean_feature=mean_feature.merge(df_top[['rank','artist']].drop_duplicates(),on='artist',how='left')
        mean_feature=mean_feature.sort_values('rank')
        return mean_feature
    
    
    def correlation(self,df):
        
        ab=df[self.features].corr().abs()
        s=ab.unstack().sort_values(kind="quicksort").reset_index().drop_duplicates(0)
        h_s=s[(s[0]<1.)&(s[0]>0.65)]
        h_sl=list(zip(h_s.level_0,h_s.level_1))
        
        print("High correlation:\n {}".format(h_s))
        
        for l in h_sl:
            plt.xlabel(l[0])
            plt.ylabel(l[1])
            plt.scatter(df[l[0]],df[l[1]])
            plt.show()
        
        return h_sl
    
    def feature_history(self,df):
    
        #weight against popularity
        #for i in self.features:
        #    df[i+'_weighted']=df[i]*df['popularity']
        #weighted_fe=[i+'_weighted' for i in self.features]
        history_year=df[df.year>2005].groupby(['year'])[self.features].mean().reset_index()
        history_year.year=history_year.year.astype(str)
        
        
        fig,ax=plt.subplots(2,4,figsize=(14,4))
        num=0
        for i in range(2):
            for j in range(4):
                
                ax[i,j].set_title(self.features[num])
                ax[i,j].plot(history_year.year,history_year[self.features[num]],'-o',c='g')
                ax[i,j].grid(True)
                ax[i,j].set_xticklabels(history_year.year,rotation=60)
                #ax[i,j].set_yticklabels(history_year[self.features[num]])
                num+=1
        fig.suptitle("Audio Features Change by Year")
        plt.tight_layout()    
        fig.subplots_adjust(top=0.85)
        plt.savefig('./images/Audio Features Change by Year.png')
        
def plot_history():
    ana=Analyze()
    df=ana.extract_data_all()
    df=ana.transform_data(df)
    ana.feature_history(df)
        
if __name__=="__main__":
    
    self=Analyze()
    df=self.feature_analysis()
    
    '''
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score,confusion_matrix,recall_score,precision_score,roc_auc_score
    from sklearn.decomposition import PCA


    self=Analyze()
    
    df=self.extract_data_all()
    df=self.transform_data(df)
    df=df[~df.analysis_url.isnull()]
    
    num_tracks=df.groupby('artist')['id'].count()
    
    
    x=df[(df.popularity>60) & (df.artist.isin(num_tracks[num_tracks>60].index))]
    x['y']=np.where(x.artist.isin(['BTS']),1,0)
    #
    X = StandardScaler().fit_transform(x[self.features])
    X = PCA(n_components=4).fit_transform(X)
    #encoder = LabelBinarizer()
    #y = encoder.fit_transform(x.artist.values)
    y=x.y.values.reshape(-1,1)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42, shuffle=True)


    model = RandomForestClassifier(n_estimators=100,criterion='entropy').fit(X_train, y_train) 
    pred = model.predict(X_test)     
    
    print("Accuracy:{:.4f}".format(accuracy_score(y_test,pred)))

        
    print("AUC:{:.4f}".format(roc_auc_score(y_test,pred)))
    print("Recall score: {:.4f}".format(recall_score(y_test,pred)))
    print("Precision score: {:.4f}".format(precision_score(y_test,pred)))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test,pred))
    
    print("F1 score: {:.4f}".format(f1_score(y_test,prediction)))
    
    '''
    