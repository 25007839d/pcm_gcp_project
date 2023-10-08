import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import configparser



if __name__ == '__main__':

     spark = SparkSession.builder\
            .master("local[1]")\
            .appName("SparkByExamples.com")\
            .getOrCreate()
