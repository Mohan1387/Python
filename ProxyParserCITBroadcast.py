import sys
import re
import datetime
import time
import csv
#import chardet // not used here but helps in geting an encoding of a character.
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col,split
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace, lower
from pyspark.sql.types import *
from pyspark.sql import functions as sf

CTIscore = spark.sparkContext.broadcast(dict(spark.sparkContext.textFile("hdfs://master:54310/user/hive/warehouse/masterlogdb.db/analytics_top_mal_domains/*.snappy",use_unicode=True).mapPartitions(lambda line: csv.reader(line,delimiter='\x01')).filter(lambda line: line[4] == 'domain').map(lambda line: line[2:4]).map(lambda x: (x[0], x[1])).collect()))

proxysample = spark.sparkContext.textFile("hdfs://master:54310/user/hdfs/Proxy/compressed/20180502/*.snappy",use_unicode=True).mapPartitions(lambda line: csv.reader(line,delimiter=' ', quotechar='"')).filter(lambda line: len(line)>3).map(lambda line: line[4:]).filter(lambda line: len(line) == 31)

def add_rating(line):
    try: 
        rate = CTIscore[line[15]]
    except Exception as e:
        rate = '0.0'
    templist = [rate]
    return line+(templist)


ratedRDD = 	proxysample.map(add_rating)

table_schema = StructType([StructField("Proxy_Device_address", StringType(), True), StructField("End_Date", StringType(), True), StructField("End_Time", StringType(), True), StructField("Device_custom_number2", StringType(), True), StructField("Device_custom_number1", StringType(), True), StructField("Source_Address", StringType(), True), StructField("Source_Port", StringType(), True), StructField("Target_Address", StringType(), True), StructField("target_Port", StringType(), True), StructField("Device_Severity", StringType(), True), StructField("DeviceEvent_Class_ID", StringType(), True), StructField("Bytes_In", StringType(), True), StructField("Bytes_Out", StringType(), True), StructField("Request_Method", StringType(), True), StructField("Source_Application_Protocol", StringType(), True), StructField("Destination_Host_Name", StringType(), True), StructField("Destination_Application_Protocol", StringType(), True), StructField("Destination_Port", StringType(), True), StructField("Request_URL_file_name", StringType(), True), StructField("Request_URL", StringType(), True), StructField("Attacker_Username", StringType(), True), StructField("Device_Custom_Strin1", StringType(), True), StructField("Device_Host_Name", StringType(), True), StructField("Target_Host_Name", StringType(), True), StructField("Device_Custom_Strin4", StringType(), True), StructField("Request_Context", StringType(), True), StructField("Request_Client_Application", StringType(), True), StructField("Device_Action", StringType(), True), StructField("Device_Event_Category", StringType(), True), StructField("Device_Custom_Strin3", StringType(), True), StructField("Device_address", StringType(), True), StructField("Rating", StringType(), True)])


df_fnl = spark.createDataFrame(ratedRDD.map(lambda x: x),schema=table_schema)

df_fnl = df_fnl.withColumn("End_Date", regexp_replace("End_date",':' , ''))

df_fnl = df_fnl.withColumn("manager_datetime", from_unixtime(unix_timestamp(sf.concat(sf.col('End_Date'),sf.lit(' '),sf.col('End_Time')), 'yyyy-MM-dd HH:mm:ss')+14400))

df_fnl = df_fnl.withColumn("partdate", regexp_replace(df_fnl['manager_datetime'].substr(0, 10),'-',''))

df_fnl.registerTempTable('tab_name')

spark.sql("SET hive.exec.compress.output=true")
spark.sql("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
spark.sql("SET mapred.output.compression.type=BLOCK")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("SET hive.exec.dynamic.partition=true")

spark.sql("insert into table masterlogdb.proxynew_tscore select Proxy_Device_address,End_Date,End_Time,Device_custom_number2,Device_custom_number1,Source_Address,Source_Port,Target_Address,target_Port,Device_Severity,DeviceEvent_Class_ID,Bytes_In,Bytes_Out,Request_Method,Source_Application_Protocol,Destination_Host_Name,Destination_Application_Protocol,Destination_Port,Request_URL_file_name,Request_URL,Attacker_Username,Device_Custom_Strin1,Device_Host_Name,Target_Host_Name,Device_Custom_Strin4,Request_Context,Request_Client_Application,Device_Action,Device_Event_Category,Device_Custom_Strin3,Device_address,Rating,manager_datetime,partdate from tab_name")



#-------------------------------------------------rough work---------------------------------------------------

CTIscore = spark.sparkContext.broadcast(spark.sql("select domainvalue, score from masterlogdb.analytics_top_mal_domains where type = 'domain' ").collect())
refdata = spark.sparkContext.parallelize(CTIscore).map(lambda x: x).toDF()

CTIrdd = CTIscore.map(lambda x: x.split('\x01'))

threat = spark.sql("select domainvalue, score from masterlogdb.analytics_top_mal_domains where type = 'domain' ").collect()
rdd = spark.sparkContext.parallelize(data)
df = rdd.map(lambda x: x).toDF()