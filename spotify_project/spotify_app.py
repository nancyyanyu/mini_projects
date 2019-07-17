#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 16:59:14 2019

@author: yanyanyu
"""
import os
import spotipy
import json
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials


class MySpotify(object):
    """This class was to download data from Spotify API.
    The download process should be: 
        artist=artist_genre -> album=album_artists(artist) -> songs=songs_albums(album)
    
    """
    def __init__(self,genre='k-pop'):
        self.authenticate()
        self.genre=genre
        
        
    def authenticate(self):
        auth=open_json('auth')
        my_id = auth['my_id']
        secret_key = auth['secret_key']
        
        client_credentials_manager = SpotifyClientCredentials(client_id = my_id, client_secret = secret_key)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
                
        
    def artist_genre(self):
        """Download all artists info from a genre """
        
        artist=[]
        at=self.sp.search(q='genre:'+self.genre,limit=50,type='artist')['artists']
        
        if at['total']>50:
            for i in range(0,at['total'],50):
                artist+=self.sp.search(q='genre:'+self.genre,limit=50,type='artist',offset=i)['artists']['items']
        save_json("data/artists/{}_artists_info".format(self.genre),artist)
        print("Download {}'s artists info".format(self.genre))
        print()
                
        return artist
    
    
    def album_artists(self):
        """Download all albums info from artists """
        artist=open_json("data/artists/{}_artists_info".format(self.genre))
        album={}
        
        for i in artist:
            name=i['name']
            album[name]=[]
            ab=self.sp.artist_albums(i['id'], limit=50)
            
            if ab['total']>50:
                for j in range(0,ab['total'],50):
                    album[name]+=self.sp.artist_albums(i['id'], limit=50,offset=j)['items']
            else:
                album[name]+=self.sp.artist_albums(i['id'], limit=50)['items']
            print("Download {}'s albums info".format(name))
        save_json("data/albums/{}_albums_info".format(self.genre),album)
        
        return album


    def songs_albums(self):  
        """Download all songs info from albums """
        album=open_json("data/albums/{}_albums_info".format(self.genre))
        
        songs={}
        
        for k,v in album.items():
            list_dir=os.listdir('./data/tracks/')
            k=k.replace('/','_')
            
            if "{}_tracks_info.json".format(k) not in list_dir:
                songs['artist']=k
                songs['albums']={}
                
                for i in v:        
                    songs['albums'][i['id']]=self.sp.album_tracks(i['id'])['items']
                save_json("data/tracks/{}_tracks_info".format(k),songs)
                print("Download {}'s tracks info".format(k))
                
        return songs
    
    
    def songs_albums_add_popularity(self):
        list_dir=os.listdir('./data/tracks/')
        
        for fl in list_dir:
            try:
                album=open_json("data/tracks/{}".format(fl[:-5]))
            except:
                pass
            for ak,av in album['albums'].items():
                for tr in av:
                    try:
                        tr['popularity']=self.sp.track(tr['id'])['popularity']
                    except Exception as e:
                        print(e,album['artist'])
            print("Add track popularity to {}'s songs".format(album['artist']))
            save_json("data/tracks/{}".format(fl[:-5]),album)
    
    
    def audio_feature(self,artist):
        """Download audio features of all tracks of one artist
        
        :artist(string)
        
        """
                
        albums=open_json('data/tracks/{}_tracks_info'.format(artist))['albums']
        Analysis=pd.DataFrame()
        for k,v in albums.items():
            audios=[]
            track_name=[]
            for i in v:
                audios.append(i['id'])
                track_name.append(i['name'])
            analysis=pd.DataFrame(self.sp.audio_features(audios))
            
            analysis['track_name']=track_name
            analysis['album']=self.sp.album(k)['name']
            analysis['release_date']=self.sp.album(k)['release_date']
            analysis['album_id']=k
            analysis['artist']=artist
            Analysis=Analysis.append(analysis)
            
        Analysis=Analysis.drop_duplicates('id')
        Analysis=Analysis[~Analysis.analysis_url.isnull()]
        
        if 0 in Analysis.columns:
            del Analysis[0]
        
        # drop similar songs, highly possible the same songs
        Analysis=Analysis.drop_duplicates(['acousticness','liveness',          
                  'instrumentalness','speechiness', 
                   'danceability','energy','valence'])
        
        print("Extract tracks analysis of artist {}".format(artist))
        
        Analysis.to_hdf("./data/analysis/{}_tracks_analysis.h5".format(artist),'analysis')
        
    def audio_feature_all(self):
        """Download audio features of all tracks of all artists of one genre
        
        """
        dir_l=os.listdir('./data/tracks/')
        for artist in dir_l:
            if artist[:-17]+'_tracks_analysis.h5' not in os.listdir('./data/analysis/'):
                try:
                    self.audio_feature(artist[:-17])
                except Exception as e:
                    print("Artist {} has something wrong:{}".format(artist,e))
        
def update_data(genre='k-pop'):
    spf=MySpotify(genre)  
    #upate artists info 
    artist=spf.artist_genre()
    #update albums info of artists
    album=spf.album_artists(artist)
    #update songs info of albums
    spf.songs_albums(album)


def save_json(name,dt):
    with open('./%s.json'%name,'w') as f:
        json.dump(dt,f)


def open_json(name):
    with open('./%s.json'%name,'r') as f:
        dt=json.load(f)
    return dt


if __name__=="__main__":
   
    #spotf=MySpotify()
    #spotf.audio_feature_all()
        
    pass
