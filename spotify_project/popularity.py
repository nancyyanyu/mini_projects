#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 12:34:47 2019

@author: yanyanyu
"""

import boto3
import json
import datetime as dt
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from spotify_app import MySpotify,open_json
from boto3.dynamodb.conditions import Key

global dynamodb 
dynamodb = boto3.resource('dynamodb')


def create_table(name='KPOP_POPULARITY'):
    
    table1 = dynamodb.create_table(
        TableName=name,
        KeySchema=[

            {
                'AttributeName': 'time',
                'KeyType': 'HASH'
            }
    
            ],
        AttributeDefinitions=[
            {
                'AttributeName': 'time',
                'AttributeType': 'S' 
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print("Create table {} success".format(name))
    print("Table status:", table1.table_status)

    
def delete_table(table_name='KPOP_POPULARITY'):
    table = dynamodb.Table(table_name)
    table.delete()


class Popularity(MySpotify):
    
    def __init__(self,table_name='KPOP_POPULARITY',genre='k-pop'):
        super().__init__(genre)
        self.table = dynamodb.Table(table_name)
        self.track_table=dynamodb.Table(table_name+'_TRACK')
        self.data_folder='%s_artists_info'%self.genre
        self.today=str(dt.datetime.now().date())
        
    def put_data(self):
        artists=open_json("data/artists/"+self.data_folder)
            
        info={}
        for artist in artists:
            info[artist['name']]={}
            info[artist['name']]['followers']=artist['followers']['total']
            info[artist['name']]['popularity']=artist['popularity']
        
        self.table.put_item(
           Item={
               'time': self.today,
               'info':info

            }
        )
        print("Update artist data to AWS DynamoDB: {}".format(self.today))
                
        
                    
    def query_popularity(self):
        
        try:

            response = self.table.query(
                KeyConditionExpression=Key('time') \
                    .eq(self.today))['Items'][0]['info']
        except:
            update_popularity()
            
        response = self.table.query(
            KeyConditionExpression=Key('time') \
                .eq(self.today))['Items'][0]['info']
        
        df=pd.DataFrame.from_dict(response,orient='index') \
                .sort_values('followers',ascending=False)
    
        return df

    def plot_popularity(self):
        df=self.query_popularity().iloc[:40,:]
        df['followers_rank']=df.followers.rank()
        df['popularity_rank']=df.popularity.rank()
        
        
        # 1st plot
        x = np.arange(len(df)) 
        width=0.35
        
        fig, ax = plt.subplots(figsize=(16,6))
        ax.bar(x - width/2, df.followers_rank.values,width, label='followers')
        ax.bar(x + width/2, df.popularity_rank.values,width, label='popularity')

        ax.set_ylabel('Score')
        ax.set_title('Followers and Popularity Rank of Top %s Groups'%self.genre.upper())
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels(df.index)
        ax.legend()
        plt.xticks(rotation=60)
        plt.tight_layout()
        plt.savefig('./images/popularity_rank_%s.png'%self.genre)
        
        # 2nd plot     
        plt.figure(figsize=(16,4))
        sns.barplot(df.index,df.followers.astype(int))
                
        plt.xticks(rotation=60)
        plt.title("Followers of Top %s Groups"%self.genre.upper())
        plt.tight_layout()
        plt.savefig('./images/followers_%s.png'%self.genre)


        # 3rd plot     
        plt.figure(figsize=(16,4))
        sns.barplot(df.index,df.sort_values('popularity',ascending=False).popularity.astype(int))
                
        plt.xticks(rotation=60)
        plt.title("Popularity of Top %s Groups"%self.genre.upper())
        plt.tight_layout()
        plt.savefig('./images/popularity_%s.png'%self.genre)

        
    def popularity_track(self,days=10):
        df=pd.DataFrame()
            
        base = dt.datetime.today()
        date_list = [str((base - dt.timedelta(days=x)).date()) for x in range(0, days)]
        for time in date_list:
            try:
                response = self.table.query(
                        KeyConditionExpression=Key('time') \
                    .eq(time))['Items'][0]['info']
                temp=pd.DataFrame.from_dict(response,orient='index').reset_index()
                temp['time']=time
                df=df.append(temp)
            except:
                print("No data for {}".format(time))
        return df
    
    def pivot_track(self):
        df=self.popularity_track()
        df=df.reset_index(drop=True)
        df_fo_pivot=df.pivot(columns='time',index='index',values='followers')
        df_po_pivot=df.pivot(columns='time',index='index',values='popularity')
        
        return df_fo_pivot,df_po_pivot
        
    def plot_track(self,n=1):
        top_artists=list(self.query_popularity().iloc[:n].index.values)
        df_fo_pivot,df_po_pivot=self.pivot_track()
        
        df_fo_pivot=df_fo_pivot[df_fo_pivot.index.isin(top_artists)]
        df_po_pivot=df_po_pivot[df_po_pivot.index.isin(top_artists)]
        #df_po_pivot=df_po_pivot.apply(lambda x:(x-x.min())/(x.max()-x.min()),axis=0)
        
        fig,ax=plt.subplots(1,2,figsize=(14,3))
        for i in df_fo_pivot.index:
            ax[0].plot(df_fo_pivot.columns,df_fo_pivot.loc[i,:],'-o',color='g',label=i)
        ax[0].set_title('Change of {}\'s number of followers'.format(i))
        ax[0].grid(True)
        for i in df_po_pivot.index:
            ax[1].plot(df_po_pivot.columns,df_po_pivot.loc[i,:],'-o',color='r',label=i)
        ax[1].set_title('Change of {}\'s popularity'.format(i))
        ax[1].grid(True)
        
        plt.savefig('./images/fol_po_change_%s.png'%self.genre)
        


def update_popularity():
    poplr=Popularity()
    poplr.artist_genre()
    poplr.put_data()
    return poplr

def plot():
    poplr=Popularity()
    poplr.plot_popularity()

if __name__=="__main__":

    #poplr=update_popularity()
    #poplr.plot_popularity()
    plot()
    pass


