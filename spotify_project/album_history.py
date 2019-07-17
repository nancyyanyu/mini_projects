#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 21:27:03 2019

@author: yanyanyu
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from spotify_app import open_json

"""
Extract album info of all artists in one genre and plot 'Number of Albums Released by Year and Month'

"""


def extract_album(folder='data/albums/k-pop_albums_info'):
    album=open_json(folder)
    df=pd.DataFrame()
    for name,ab in album.items():
        temp=pd.DataFrame(ab)
        temp['artist']=name
        df=df.append(temp)
        
        print("Extract albums info from {}".format(name))
    return df


def plot_album_history(df):
    """Plot Number of Albums Released by Year and Month use the release_date of all albums"""
    df=df[df.album_type=='album'].sort_values('release_date')
    df=df[df.release_date.str.len()==10]
    df['year_month']=[i[:7] for i in df.release_date]
    df['year']=[int(i[:4]) for i in df.release_date]
    df['month']=[int(i[5:7]) for i in df.release_date]
    
    df=df[(df.year<2019)&(df.year>2000)]
    count_year_month=df.groupby(['year','month'])['name'].count().reset_index()
    count_year=df.groupby('year')['name'].count()
    count_month=df.groupby('month')['name'].count()
        
    
    fig, ax = plt.subplots(3,1,figsize=(12,12))
    ax[0].set_title("Number of Albums Released by Year")
    rect=ax[0].plot(count_year,'o-')
    ax[0].set_xlabel('Year')
    ax[0].grid(True)
    ax[0].set_xticks(count_year.index)
    ax[0].set_xticklabels(count_year.index)
    
    change=list(count_year.pct_change().round(2).values)
    for i in range(1,len(count_year)):
        if change[i]>=0:
            anno='+{0:.00%}'.format(change[i])
        else:
            anno='-{0:.00%}'.format(change[i])

        ax[0].annotate('{}'.format(anno),
                xy=(rect[0].get_xdata()[i],rect[0].get_ydata()[i]), 
                xytext=(0,3),# 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom')

    ax[1].set_title("Number of Albums Released by Month")
    ax[1].bar(count_month.index,count_month.values,0.75,color='g')
    ax[1].set_xlabel('Month')
    ax[1].set_xticks(count_month.index)
    ax[1].grid(True)

    ax[2].scatter(count_year_month.year.astype(str), count_year_month.month, c= count_year_month.name,s=count_year_month.name*10, alpha=0.8)
    ax[2].set_yticks(np.arange(1,13))
    ax[2].grid(True)
    ax[2].set_title('Number of Albums Released by Year and Month')
    ax[2].set_xlabel('Year')
    ax[2].set_ylabel('Month')

    fig.tight_layout()
    
    plt.savefig('./images/Number of Albums Released by Year and Month.png')
    plt.show()



if __name__=="__main__":

    
    df=extract_album()
    plot_album_history(df)
    pass

