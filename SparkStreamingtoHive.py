#__author__ = 'Mohan Manivannan' 
#__name__ = 'Spark Streaming to insert data to hive Using PySpark'
#__Version__ = '1.0'
#Create on 2nd April 2018 Version 1.0

from pyspark.sql.functions import col,split
import re
import datetime
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace, lower
from pyspark.sql.types import *
from pyspark.sql import SparkSession, HiveContext
from pyspark.streaming import StreamingContext

#spark session driver configuration
spark = (SparkSession.builder.appName("OWALogParser").config("SPARK_WORKER_MEMORY","6g").config("spark.executor.cores","4").config("spark.driver.memory", "2g").config("spark.executor.memory", "2g").config("hive.metastore.uris", "thrift://master:9083").config("spark.sql.hive.metastore.version", "2.1.1").config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").config("spark.sql.hive.metastore.sharedPrefixes", "com.mysql.jdbc").config("spark.sql.warehouse.dir", "hdfs://master:54310/user/hive/warehouse").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.compress.output", "true").config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec").config("mapred.output.compression.type", "BLOCK").enableHiveSupport().getOrCreate())

# Create a local StreamingContext with batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

#covert the Dstream into RDD of list
words = lines.map(lambda line: line.split(","))

#function to insert each rdd batch accumulated over given time interval into hive table
def send_df(rdd):
    #Define Schema for the Dateframe
    table_schema = StructType([StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("designation", StringType(), True)])
    # Convert RDD  to  DataFrame
    wordsDataFrame = spark.createDataFrame(rdd.map(lambda x: x),schema=table_schema)

    # Creates a temporary view using the DataFrame
    wordsDataFrame.createOrReplaceTempView("words")

    # insert data into table using SQL
    wordCountsDataFrame = spark.sql("insert into table test.sparkstream_test select * from words")
    #wordCountsDataFrame.show()

words.foreachRDD(send_df)

#optionally print each RDD batch just to test
words.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()

#spark-submit /home/hduser/pyspark_script/streamingTest.py localhost 9999
