#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 23:04:37 2019

@author: yanyanyu
"""




class Log4j(object):
    def __init__(self,spark):
        conf=spark.sparkContext.getConf()
        app_id=conf.get('spark.app.id')
        app_name=conf.get('spark.app.name')              
        log4j=spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self,message):
        self.logger.error(message)
        
    def info(self,message):
        self.logger.info(message)
        
    def warn(self,message):
        self.logger.warn(message)

    