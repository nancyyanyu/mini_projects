#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 23:53:58 2019

@author: yanyanyu
"""


from spark import start_spark
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import Row


def main():
    spark,conf=start_spark()
    steps_per_floor_=conf['steps_per_floor']
    pass

def extract(spark):
    df=spark.read.parquet('tests/test_data/employees')
    return df

def transform(df,steps_per_floor_,spark):
    df.createOrReplaceTempView("table1")
    df_transformed=spark.sql("select id, concat(first_name,' ' , second_name) as name, floor* %s as steps_to_desk from table1"%steps_per_floor_)

    return df_transformed

def load(df):
    df.coalesce(1).write.csv('loaded_data', mode='overwrite', header=True)


def create_test_data(spark,conf):
   
    local_records=[
            Row(id=1, first_name='nancy', second_name="yan", floor=1),
            Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
            Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
            Row(id=4, first_name='Ken', second_name='Lai', floor=2),
            Row(id=5, first_name='Stu', second_name='White', floor=3),
            Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
            Row(id=7, first_name='Phil', second_name='Bird', floor=4),
            Row(id=8, first_name='Kim', second_name='Suter', floor=4)
        ]
    
    df=spark.createDataFrame(local_records)
    df_tf=transform(df,conf['steps_per_floor'],spark)
    df_tf.coalesce(1).write.parquet('tests/test_data/employees_report',mode='overwrite')
    






