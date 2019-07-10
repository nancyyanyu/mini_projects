#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 10:42:17 2019

@author: yanyanyu
"""

import twitter
import socket

TCP_IP = "localhost"
TCP_PORT = 9876
KEY_WORD = 'Trump'

def twt_app(TCP_IP,TCP_PORT,keyword=KEY_WORD):

    consumer_key='Ee3rFDQkGja4LlCQVV2NSb161'
    consumer_secret='MoUycihwXsisMc1S6Ok7BdqroNJdcPadVOzyYKGkvTqdtl2rkf'
    access_token='837607037404078080-Ndt0u3TGyrt1a0lqPzeRvPnqV0uMMb7'
    access_token_secret='s7Nv9lMe0Tk1SaTtLGb97p8kA82xEUGif04S3EK2u4JWY'
    
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





