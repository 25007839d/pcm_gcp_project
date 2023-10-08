import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import storage
import os
os.environ["GCLOUD_PROJECT"]="formal-purpose-352005"




spark = SparkSession.builder\
        .master("local[1]") \
        .config('parentProject', 'formal-purpose-352005')\
        .appName("SparkByExamples.com")\
        .getOrCreate()


def null_dropmalformed(schema,link):
     '''
    PERMISSIVE, DROPMALFORMED, FAILFAST
    null validation of integer col or already string have null value
    :param schema: pass structield schema
    :param link: csv file path
    :return: dataframe
     '''

     df = spark.read \
         .option("mode", "DROPMALFORMED") \
         .option('columnNameOfCorruptRecord', "bad_record") \
         .csv(link, schema=schema,header=True).drop(col("bad_record"))


     return df


def null_PERMISSIVE_data(schema,link):
    '''
    # PERMISSIVE, DROPMALFORMED, FAILFAST
    # null validation of integer col or already string have null value
    :param schema: pass structield schema
    :param link: csv file path
    :return: dataframe
    '''


    df = spark.read \
             .option("badRecordsPath","bad_data.txt")\
             .option("mode", "PERMISSIVE") \
             .option("delimiter",",")\
             .csv(link,schema=schema,header=True)


    return df

def drop_na(df):
    '''
    :param df: pass the data frame
    :return: data frame null validation
    '''
    null_val= df.dropna('any')
    return null_val


def manual_df(schema,link):
    '''
     no null validation
     simple dataframe created
    :param schema: pass structield schema
    :param link: csv file path
    :return: dataframe
    '''
    df = spark.read.csv(link, schema=schema,header=True).drop(col("bad_record"))
    return df

def gcs_df(link):
    '''
     no null validation
     simple dataframe created
    :param schema: pass structield schema
    :param link: gcs csv file path
    :return: dataframe
    '''
    obj_clint = storage.Client.from_service_account_json(r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json")

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                         r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json")  # this is vry important hadoopconfiguration


    # df = spark.read.csv(r"gs://pcm_dev-ingestion1/data/my_sql/*.csv", header=True)
    # df.show()
    gcsdf = spark.read.csv(link, header=True)
    return gcsdf
a= gcs_df(r"gs://etl-d-r/department_2022_05_22.csv")
a.show()

# # # PERMISSIVE, DROPMALFORMED, FAILFAST
#      df = spark.read\
#          .option("badRecordsPath","bad_data.txt")\
#          .option("mode", "PERMISSIVE") \
#          .option('columnNameOfCorruptRecord',"bad_record")\
#          .option("delimiter",",")\
#          .csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata",schema=schema,header=True)
#      df.show()

