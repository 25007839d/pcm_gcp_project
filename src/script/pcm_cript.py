
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import storage,bigquery
import os
import logging
import configparser



if __name__ == '__main__':

     # spark = SparkSession.builder\
     #        .master("local[1]")\
     #        .appName("SparkByExamples.com")\
     #        .getOrCreate()

     spark = SparkSession.builder \
          .master('local[*]').appName('spark-bigquery-demo').getOrCreate()


# config file input path -------------------------------------------------------------
     config = configparser.ConfigParser()
     config.read(r'../config/pcm_confg.ini')
     inputfile      = config.get('path','rawdatapath')
     jsonschema     = config.get('path','jsonschmapath')
     spcharval      = config.get('column','spcharval').split(',')
     nullvalidation = config.get('column','nullvalidation').split(',')
     mobnumval      = config.get('column','mobnumval').split(',')
     emailval       = config.get('column','emailval').split(',')
     dateval        = config.get('column','dateval').split(',')
     baddatawritepath=config.get('path','baddatawritepath')
     gooddatawritepath = config.get('path', 'gooddatawritepath')
     logwritepath      = config.get('path','logwritepath')
     gcspath           = config.get('path','gcspath')

# create file record logger program------------------------------------------------

     pcm_logger = logging.getLogger("pcm.log")
     pcm_logger.setLevel(logging.DEBUG)

     #    call log fun for create new file with current time
     from util.log_new_file_fun import logfun
     from datetime import datetime

     log_path = logfun(logwritepath)
     print(log_path)
     '''
     new log path creater
     '''
     pcm = logging.FileHandler(log_path)
     pcm.setLevel(logging.DEBUG)
     pcm_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
     pcm.setFormatter(pcm_formater)
     pcm_logger.addHandler(pcm)




#create data frame in auto-------------------------------------------------------------
     from util.Auto_schema_col_val import Autodf

     '''
     /function/
     create_df_auto     : only create df in header = true
     special_char_rem_df:  all special char remove from column name
     rep_any_df         : can replace any column char...

     '''
     a = Autodf(inputfile)  # creat df as we as
     a.create_df_auto()

     pcm_logger.info("auto df created but not in used")

# create structfield schema-------------------------------------------------------------
     from util.json_schema import schema1

     '''
     json file convert in to structtype schema
     '''

     j_schema =schema1(jsonschema)
     print(j_schema)
     pcm_logger.info("json schema converted into structtype")
#manula schema df call-------------------------------------------------------------------
     from util.Man_df_dropmal_dropna_val import manual_df

     '''
     manual df create without null validation
     '''

     manualdf = manual_df(schema=j_schema,link=inputfile)


     pcm_logger.info("manual dataframe created")

# gsc file df call-----------------------------------------------------------------------
     from util.Man_df_dropmal_dropna_val import gcs_df

     gcsdf= gcs_df(schema=j_schema,link=gcspath)
     gcsdf.show()

     print('gcs csv read')
# null value validation with dropna and dropmal--------------------------------------------
#      from util.Man_df_dropmal_dropna_val import null_dropmalformed
#      '''
#      manual df create with dropmalformed and all null row's droped
#      '''
#
#      df = null_dropmalformed(schema=j_schema,link=inputfile)

     pcm_logger.info("data frame created by dropmalformed and null value validation done// but not in use")



# only null validation by dropna df-drop the null column---------------------------------------
##########################################################################################
     # from util.Man_df_dropmal_dropna_val import drop_na
     # df1 = drop_na(manualdf)
     # df1.show()
     pcm_logger.info(' drop na not in use')


# special char validation----------------------------------------------------------------------

     # spchar = udf(lambda z: CharValidation.specialcharvalidation(z), StringType())
     # df2 = df.withColumn("name", spchar(col("name"))).withColumn('email', spchar(col('email')))\
     #      .where(col("name") != '').where(col("email") != '')
     # df2.show() # add where col for filter null value

     from util.validation import spec_char_val
     '''
     validation by udf
     '''
     spcvdf = spec_char_val(df=gcsdf,col_nam=spcharval)

     pcm_logger.info("spc char validation done")

#mobile number validation ---------------------------------------------------------------------

     from util.validation import mob_num_val
     '''
     validation by udf
     '''
     mobvdf = mob_num_val(df=spcvdf,col_nam=mobnumval)

     pcm_logger.info("mobile number val done")

# email validation-----------------------------------------------------------------------------------------
     from util.validation import email_val
     evdf=email_val(df=mobvdf,col_nam=emailval)

     pcm_logger.info("email val done")

#nullval----------------------------------------------------------------------------------------------------
     from util.validation import null_val

     nvdf = null_val(df=evdf, col_nam=nullvalidation)
     nvdf.show()
     print("good data ")
     pcm_logger.info("null val done")


#filter condition if we want -------------------------------------------------------------------

     # Replace empty string with None value
     # from pyspark.sql.functions import col, when
#null value filter after specal validation
     # df4 =df3.where(df3.name != '').where(df3.email != '').show()


     # print('none value add')

#all / whatever we want null value replacement in all column ------------------------------------
     # df4=df3.na.fill({'name': 'abc', 'email': 'er.xxxxx', 'age': '18'}) \
     #     .replace('null', '188').replace('null', '187').replace('null', 'bad')  # method 1

     # df3.na.fill(value='100', subset=["age"]).fillna(value='bbnbn', subset=["email"]).show() # metod 2 only computer null change
     # df3.show()

#  bad data collect --------------------------------------------------------------------------


     bdf= manualdf.join(nvdf,"dept_id", "leftanti")  # join the filter df and manual df
     bdf.show()
     print("bad data")
     pcm_logger.info("bad data collect ")


# write the bad and good data at target

     bdf.write.mode("overwrite").option('header',True).csv(baddatawritepath)

     pcm_logger.info("bad data write ")
##gooddata--------------------------------------------------------------

     nvdf.write.mode("overwrite").option('header',True).csv(gooddatawritepath)
     pcm_logger.info("good data write ")

# bigquery connector load good data to BQ table

     from ddl.load_data_BQ_GCS import good_pcm_load_BQ

     good_pcm_load_BQ(nvdf)


# bad data load into gcs location

     from ddl.load_data_BQ_GCS import bad_pcm_load_gcs
     bad_pcm_load_gcs(bdf)


