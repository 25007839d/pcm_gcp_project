
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder\
        .master("local[*]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()
# Nested Class


class Autodf():

    def __init__(self,datalink):
        self.a= datalink



    def create_df_auto(self):
        df = spark.read.csv(self.a, header=True)
        return df


    def special_char_rem_df (self):
        df = spark.read.csv(self.a, header=True)
        df1 = df.toDF(*[re.sub('[^\w]', ' ', c) for c in df.columns])
        return df1

    def rep_any_df(self,find,replace):
        self.find=find
        self.replace=replace

        df = spark.read.csv(self.a, header=True)
        df1 = df.toDF(*[re.sub(self.find,self.replace, c) for c in df.columns])
        return df1



a = Autodf(r"D:\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv")















































