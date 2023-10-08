import findspark
from google.cloud.storage import blob
from google.cloud import bigquery
findspark.init()
from pyspark.sql import SparkSession
from google.cloud import storage
import os
os.environ["GCLOUD_PROJECT"]="formal-purpose-352005"



spark = SparkSession.builder\
        .master("local[1]") \
        .config('parentProject', 'formal-purpose-352005')\
        .appName("SparkByExamples.com")\
        .getOrCreate()

def good_pcm_load_BQ(gdf):
            # '''
            #
            # :param gdf: which data frame we want to load on bq
            # :return: no return
            # we can do all soft coading of gcp, gcs,bq lnk
            # '''

        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                         r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json")
        obj_clint = storage.Client.from_service_account_json(r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json")
        bucket = "etl-d-r"
        spark.conf.set('temporaryGcsBucket', bucket)


        # df.write.format('bigquery') \
        # .option('table', 'ritu-351906:bwt_session.try5') \
        # .save()

        gdf.write.format('bigquery') \
          .option('table', 'formal-purpose-352005:etl.tbl_85') \
          .mode('append').save()

        print("good data load into BQ table")
def bad_pcm_load_gcs(bdf):

        bdf.write.mode("append").option('header', True).csv("gs://etl-d-r/bad_data/")


        print('bad data write on gcs folder')






#  spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", key)
     #  spark.conf.set('temporaryGcsBucket', temp_buck)
     #
     #
     #
     # nvdf.write.format('bigquery') \
     #         .option('table','{}:{}.{}'.format(project_id,dataset,filename)) \
     #         .mode('append').save()

     # credentals_path =r"C:\Big Data\ritu-351906-27e10a6678af.json"
     # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentals_path
     # client = bigquery.Client()
     # table_id = 'ritu-351906.bwt_session1.try'
     #
     # spark = SparkSession.builder \
     #      .config('spark.jars.packages',
     #              'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
     #      .master('local[*]').appName('spark-bigquery-demo').getOrCreate()
     # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", r"C:\Big Data\ritu-351906-27e10a6678af.json")

     # bucket = "pcm_dev-ingestion1"
     # spark.conf.set('temporaryGcsBucket', bucket)
     #
     # # nvdf.write.format('bigquery') \
     # # .option('table', 'ritu-351906:bwt_session.pcm') \
     # # .save()
     #
     # nvdf.write.format('bigquery') \
     #      .option('table', 'ritu-351906:bwt_session.pcm') \
     #      .mode('append').save()