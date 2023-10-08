import findspark
findspark.init()
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,DateType,StringType
spark= SparkSession.builder.master('local[*]').appName('pcm_project').getOrCreate()
def schema1(schema_arg):
    '''

    :param schema_arg: pass the json schema file path
    :return: structfield type
    '''
    schema=StructType([])

    data_types={
        "integer":IntegerType(),
        "string":StringType(),
        "date":DateType(),
        "integer":IntegerType()

            }
    with open(schema_arg,'r')as jf:
        json_schema = jf.read() # read the file
        '''
        
        '''
        temp_schema=json_schema.strip('[]').replace('\n','').replace(' ','').replace('},{','}},{{').split('},{')
        schema_rdd1=spark.sparkContext.parallelize(temp_schema)
        schema_rdd=schema_rdd1.map(lambda x:json.loads(x)).collect()
        for i in schema_rdd:

            schema.add(i.get("name"),data_types.get(i.get('type')))

    return schema



# a= schema1(r'C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\schema\schema.json')
# print(a)

# df= spark.read.csv(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv",schema=a)
# df.show()

# # Opening JSON file to distonary
#
#
#
# import json
#
# # Opening JSON file to distonary
# f = open(r'C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\schema\schema.json')
# myjson = f.read()
# print(myjson)
# aList = json.loads(myjson)
#
#
# str = StructType()
# data_types={
#         "integer":IntegerType(),
#         "string":StringType(),
#         "date":DateType(),
#         "integer":IntegerType()}
# for i in aList:
#     str.add(i.get("name"),data_types.get(i.get('type')))
# print(str)



# df3 = spark.createDataFrame(spark.sparkContext.txt(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\*.csv",schema=aList))
# df3.show()