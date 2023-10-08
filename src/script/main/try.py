import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import storage,bigquery
import os
import logging
import configparser

spark = SparkSession.builder.master('local[*]').getOrCreate()

df= spark.read.csv(r'C:\Users\Dell\Desktop\data_file.csv')
df.show()

df.withColumn('nullcount',sum(df[col].isNull().cast('integer') for col in df.columns)).show()