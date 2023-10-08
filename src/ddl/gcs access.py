import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("GCSFilesRead").getOrCreate()

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","charming-shield-350913-ecccf1d79c4b.json")

bucket_name="my_bucket"
path=f"gs://pcm_dev-ingestion/data/my_sql/data.csv"

df=spark.read.csv(path, header=True)
df.show()


from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "my-new-bucket"

# Creates the new bucket
bucket = storage_client.create_bucket(bucket_name)

load = storage_client.w