'''
Created on Feb 26, 2017

@author: ctsbala

'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

spark = SparkSession \
    .builder \
    .appName("LoadOrder") \
    .getOrCreate()
        
sc = spark.sparkContext

lines = sc.textFile("s3n://aws_bucket/lineorder-multi.tbl0000_part_00.gz")

parts = lines.map(lambda l: l.split('|'))
lineorders = parts.map(lambda p: Row(col1=p[0], col2=p[1],col3=p[2], \
                                        col4=p[3],col5=p[4],col6=p[5],\
                                        col7=p[6],col8=p[7],col9=p[8],col10=p[9],col11=p[10],col12=p[11],col13=p[12],\
                                        col14=p[13],col15=p[14],col16=p[15],col17=p[16]))

# Infer the schema, and register the DataFrame as a table.
slackMessagesDF = spark.createDataFrame(lineorders)

resultDF=slackMessagesDF.groupBy("col17").count()

resultDF.show()

resultRDD=resultDF.rdd

resultRDD.repartition(1).saveAsTextFile("s3n://aws_bucket/SparkLineOrderout")
