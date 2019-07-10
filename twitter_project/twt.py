#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 10:42:17 2019

@author: yanyanyu
"""

import twitter
import socket
import json
TCP_IP = "localhost"
TCP_PORT = 9876
KEY_WORD = 'Trump'

def twt_app(TCP_IP,TCP_PORT,keyword=KEY_WORD):
    with open('./OAuth.json','r') as f:
        oauth=json.load(f)
    consumer_key=oauth['consumer_key']
    consumer_secret=oauth['consumer_secret']
    access_token=oauth['access_token']
    access_token_secret=oauth['access_token_secret']
    
    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=access_token,
                      access_token_secret=access_token_secret,
                      sleep_on_rate_limit=True)

    LANGUAGES = ['en']
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(10)
    print("Waiting for TCP connection...")
    
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    
    for line in api.GetStreamFilter(track=[keyword],languages=LANGUAGES):
        conn.send( line['text'].encode('utf-8') )
        print(line['text'])
        print()
        
if __name__=="__main__":
    
    twt_app(TCP_IP,TCP_PORT,keyword=KEY_WORD)





