from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
sparkSession = (SparkSession.builder.appName('hello').enableHiveSupport().getOrCreate())
df_load = sparkSession.sql('select * from phishing_attack.temp_url_categorization_master')
codec = "org.apache.hadoop.io.compress.SnappyCodec"
rdd_load = df_load.rdd.map(tuple)rdd_load.saveAsTextFile("/user/hdfs/test/trail",codec) 
