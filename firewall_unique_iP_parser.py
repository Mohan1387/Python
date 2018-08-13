#__author__ = 'Mohan Manivannan'
#__name__ = 'Application to parse Firewall IPs logs'
#__Version__ = '1.0'
#Create on 8th AUG 2018 Version 1.0

from pyspark.sql.functions import col,split
import re
import datetime
import sys
import time
import csv
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace, lower
from pyspark.sql.types import *
from pyspark.sql import SparkSession, HiveContext

spark = (SparkSession.builder.appName("FirwallLogParser").config("SPARK_WORKER_MEMORY","6g").config("spark.executor.cores","2").config("spark.driver.memory", "6g").config("spark.executor.memory", "6g").config("hive.metastore.uris", "thrift://master:9083").config("spark.sql.hive.metastore.version", "2.1.1").config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").config("spark.sql.hive.metastore.sharedPrefixes", "com.mysql.jdbc").config("spark.sql.warehouse.dir", "hdfs://master:54310/user/hive/warehouse").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.compress.output", "true").config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec").config("mapred.output.compression.type", "BLOCK").enableHiveSupport().getOrCreate())

#Get Input path from the passed parametter
inputfile = sys.argv[1]
#dateval = str(sys.argv[2])

#Read firewall ips log from HDFS
firewall = spark.sparkContext.textFile(inputfile,use_unicode=True).mapPartitions(lambda line: csv.reader(line, delimiter='\t', quotechar='"'))

#---------------------Create DataFrame

#Define schema
table_schema = StructType([StructField("event_deviceAddress", StringType(), True), StructField("event_sourceAddress", StringType(), True), 
StructField("event_sourceTranslatedAddress", StringType(), True), StructField("event_destinationAddress", StringType(), True),
StructField("event_destinationTranslatedAddress", StringType(), True), StructField("partdate", StringType(), True)])

#Create dataframe using the schema							
df_fnl = spark.createDataFrame(firewall.map(lambda x: x),schema=table_schema)

#Register the DataFrame as temp table
df_fnl.registerTempTable('tab_name')

spark.sql("insert into table masterlogdb.du_day_unique_ip select a.ip, a.partdate from (select distinct event_deviceAddress as ip, partdate from tab_name union select distinct event_sourceAddress as ip, partdate from tab_name union select distinct event_sourceTranslatedAddress as ip, partdate from tab_name union select distinct event_destinationAddress as ip, partdate from tab_name union select distinct event_destinationTranslatedAddress as ip, partdate from tab_name) a distribute by a.partdate")
