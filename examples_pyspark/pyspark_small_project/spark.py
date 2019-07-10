#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 22:00:05 2019

@author: yanyanyu
"""

import __main__

from os import environ,listdir,path
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import json


def start_spark(app_name="my_spark_app",master="local[*]",files=['etl_conf.json']):


    flag_repl=not(hasattr(__main__,'__file__'))
    flag_debug='DEBUG' in environ.keys()
        
        
    if not(flag_repl or flag_debug):    
        spark_builder=(SparkSession.builder.appName(app_name))
    else:
        spark_builder=SparkSession.builder.appName(app_name).master(master)
    
    spark_files='.'.join(list(files))
    spark_builder.config('spark.files',spark_files)
    spark_builder.config(conf=SparkConf())    
    spark_sess=spark_builder.getOrCreate()
    
    
    #spark_logger=logger.Log4j(spark_sess)
    spark_files_dir=SparkFiles.getRootDirectory()
    
    config_files=[x for x in listdir(spark_files_dir) if x.endswith('conf.json')]
    
    if config_files:
        path_to_config_file=path.join(spark_files_dir,config_files[0])
        with open(path_to_config_file,'r') as f:
            config_dict=json.load(f)
    else:
        
        config_dict=None
    
    return spark_sess,config_dict


