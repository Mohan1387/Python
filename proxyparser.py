/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-2 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-2 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-3 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-3 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-4 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-4 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-5 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-5 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-6 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-6 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-7 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-7 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-8 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-8 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig
/usr/local/pig/bin/pig -param FILE='/user/hdfs/OWA/uncompressed/eventlog.csv.'$(date -d "-9 days" +%Y%m%d)'*' -param DATE='/user/hdfs/OWA/compressed/'$(date -d "-9 days" +%Y%m%d) -f /home/hduser/pig_scripts/FileCompress.pig


dvar=$(date -d "-1 days"  +%Y%m%d)
spark-submit --driver-memory 20g /home/hduser/pyspark_script/OWAParser.py hdfs://master:54310/user/hdfs/OWA/compressed/$dvar/*.snappy $dvar


val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
**// Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).**
val accuracy = evaluator.evaluate(predictions)


spark-submit  --master yarn --executor-cores 2  --num-executors 8 --driver-memory 20g --conf spark.rpc.message.maxSize=2000 --conf spark.dynamicAllocation.enabled=false  /home/hduser/pyspark_script/OWAParser.py hdfs://master:54310/user/hdfs/OWA/compressed/$dvar/*.snappy $dvar


dvar=$(date -d "-1 days"  +%Y%m%d)
spark-submit  --master yarn --executor-cores 4  --num-executors 8 --driver-memory 10g /home/hduser/pyspark_script/OWAParser.py hdfs://master:54310/user/hdfs/OWA/compressed/$dvar/*.snappy

dvar=$(date -d "-1 days"  +%Y%m%d)
spark-submit  --master yarn --executor-cores 4  --num-executors 8 --driver-memory 10g /home/hduser/pyspark_script/ProxyparserTest.py hdfs://master:54310/user/hdfs/Proxy/compressed/$dvar/*.snappy

import sys
#reload(sys)
#sys.setdefaultencoding('utf8')
--------------------------------------------------------------working code ---------------------------------------------------------------
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

proxysample = spark.sparkContext.textFile("hdfs://master:54310/user/hdfs/Proxy/compressed/20180502/*.snappy",use_unicode=True).mapPartitions(lambda line: csv.reader(line,delimiter=' ', quotechar='"')).filter(lambda line: len(line)>3).map(lambda line: line[4:]).filter(lambda line: len(line) == 31)

table_schema = StructType([StructField("Proxy_Device_address", StringType(), True), StructField("End_Date", StringType(), True), StructField("End_Time", StringType(), True), StructField("Device_custom_number2", StringType(), True), StructField("Device_custom_number1", StringType(), True), StructField("Source_Address", StringType(), True), StructField("Source_Port", StringType(), True), StructField("Target_Address", StringType(), True), StructField("target_Port", StringType(), True), StructField("Device_Severity", StringType(), True), StructField("DeviceEvent_Class_ID", StringType(), True), StructField("Bytes_In", StringType(), True), StructField("Bytes_Out", StringType(), True), StructField("Request_Method", StringType(), True), StructField("Source_Application_Protocol", StringType(), True), StructField("Destination_Host_Name", StringType(), True), StructField("Destination_Application_Protocol", StringType(), True), StructField("Destination_Port", StringType(), True), StructField("Request_URL_file_name", StringType(), True), StructField("Request_URL", StringType(), True), StructField("Attacker_Username", StringType(), True), StructField("Device_Custom_Strin1", StringType(), True), StructField("Device_Host_Name", StringType(), True), StructField("Target_Host_Name", StringType(), True), StructField("Device_Custom_Strin4", StringType(), True), StructField("Request_Context", StringType(), True), StructField("Request_Client_Application", StringType(), True), StructField("Device_Action", StringType(), True), StructField("Device_Event_Category", StringType(), True), StructField("Device_Custom_Strin3", StringType(), True), StructField("Device_address", StringType(), True)])
	
	
df_fnl = spark.createDataFrame(proxysample.map(lambda x: x),schema=table_schema)

df_fnl = df_fnl.withColumn("End_Date", regexp_replace("End_date",':' , ''))

df_fnl = df_fnl.withColumn("manager_datetime", from_unixtime(unix_timestamp(sf.concat(sf.col('End_Date'),sf.lit(' '),sf.col('End_Time')), 'yyyy-MM-dd HH:mm:ss')+14400))

df_fnl = df_fnl.withColumn("partdate", regexp_replace(df_fnl['manager_datetime'].substr(0, 10),'-',''))

df_fnl.registerTempTable('tab_name')

spark.sql("SET hive.exec.compress.output=true")
spark.sql("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
spark.sql("SET mapred.output.compression.type=BLOCK")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("SET hive.exec.dynamic.partition=true")

spark.sql("insert into table masterlogdb.proxynew select Proxy_Device_address,End_Date,End_Time,Device_custom_number2,Device_custom_number1,Source_Address,Source_Port,Target_Address,target_Port,Device_Severity,DeviceEvent_Class_ID,Bytes_In,Bytes_Out,Request_Method,Source_Application_Protocol,Destination_Host_Name,Destination_Application_Protocol,Destination_Port,Request_URL_file_name,Request_URL,Attacker_Username,Device_Custom_Strin1,Device_Host_Name,Target_Host_Name,Device_Custom_Strin4,Request_Context,Request_Client_Application,Device_Action,Device_Event_Category,Device_Custom_Strin3,Device_address,manager_datetime,partdate from tab_name")



------------------------------------------------------end of code------------------------------------------------------------------------------------


def checkencode(x):
    if len(x) == 31:
        chk = chardet.detect(x)
        print(chk['encoding'])
        if chk['encoding'] != 'ascii':
            #print(chk['encoding'])
            #print(x)
            lts = [chk['encoding']]
            return

tempres = proxysample.map(checkencode)

tempres.take(10000000)







asciiascii



from chardet.universaldetector import UniversalDetector

def checkencode(x):
    for i in range(0,len(x)):
        UniversalDetector.feed(x[i])
        if UniversalDetector.done:
            break
    UniversalDetector.close()
    if UniversalDetector.result['confidence'] != 1.0:
	    print(x)
        return x

tempres = proxysample.map(checkencode)

tempres.take(10000000)

def iterate_rdd(x):
    templist = []
    try:
        if len(x) == 31:
            if not isinstance(x[0], unicode):
                x[0] = unicode(x[0], "utf-8")
                templist.append(x[0].encode("utf-8"))
            else:
                templist.append(x[0].encode("utf-8"))
            if not isinstance(x[1], unicode):
                x[1] = unicode(x[1], "utf-8")
                templist.append(x[1].encode("utf-8"))
            else:
                templist.append(x[1].encode("utf-8"))
            if not isinstance(x[2], unicode):
                x[2] = unicode(x[2], "utf-8")
                templist.append(x[2].encode("utf-8"))
            else:
                templist.append(x[2].encode("utf-8"))
            if not isinstance(x[3], unicode):
                x[3] = unicode(x[3], "utf-8")
                templist.append(x[3].encode("utf-8"))
            else:
                templist.append(x[3].encode("utf-8"))
            if not isinstance(x[4], unicode):
                x[4] = unicode(x[4], "utf-8")
                templist.append(x[4].encode("utf-8"))
            else:
                templist.append(x[4].encode("utf-8"))
            if not isinstance(x[5], unicode):
                x[5] = unicode(x[5], "utf-8")        
                templist.append(x[5].encode("utf-8"))
            else:
                templist.append(x[5].encode("utf-8"))
            if not isinstance(x[6], unicode):
                x[6] = unicode(x[6], "utf-8")
                templist.append(x[6].encode("utf-8"))
            else:
                templist.append(x[6].encode("utf-8"))
            if not isinstance(x[7], unicode):
                x[7] = unicode(x[7], "utf-8")
                templist.append(x[7].encode("utf-8"))
            else:
                templist.append(x[7].encode("utf-8"))
            if not isinstance(x[8], unicode):
                x[8] = unicode(x[8], "utf-8")
                templist.append(x[8].encode("utf-8"))
            else:
                templist.append(x[8].encode("utf-8"))
            if not isinstance(x[9], unicode):
                x[9] = unicode(x[9], "utf-8")
                templist.append(x[9].encode("utf-8"))
            else:
                templist.append(x[9].encode("utf-8"))
            if not isinstance(x[10], unicode):
                x[10] = unicode(x[10], "utf-8")
                templist.append(x[10].encode("utf-8"))
            else:
                templist.append(x[10].encode("utf-8"))
            if not isinstance(x[11], unicode):
                x[11] = unicode(x[11], "utf-8")
                templist.append(x[11].encode("utf-8"))
            else:
                templist.append(x[11].encode("utf-8"))
            if not isinstance(x[12], unicode):
                x[12] = unicode(x[12], "utf-8")
                templist.append(x[12].encode("utf-8"))
            else:
                templist.append(x[12].encode("utf-8"))
            if not isinstance(x[13], unicode):
                x[13] = unicode(x[13], "utf-8")
                templist.append(x[13].encode("utf-8"))
            else:
                templist.append(x[13].encode("utf-8"))
            if not isinstance(x[14], unicode):
                x[14] = unicode(x[14], "utf-8")
                templist.append(x[14].encode("utf-8"))
            else:
                templist.append(x[14].encode("utf-8"))
            if not isinstance(x[15], unicode):
                x[15] = unicode(x[15], "utf-8")
                templist.append(x[15].encode("utf-8"))
            else:
                templist.append(x[15].encode("utf-8"))
            if not isinstance(x[16], unicode):
                x[16] = unicode(x[16], "utf-8")
                templist.append(x[16].encode("utf-8"))
            else:
                templist.append(x[16].encode("utf-8"))
            if not isinstance(x[17], unicode):
                x[17] = unicode(x[17], "utf-8")
                templist.append(x[17].encode("utf-8"))
            else:
                templist.append(x[17].encode("utf-8"))
            if not isinstance(x[18], unicode):
                x[18] = unicode(x[18], "utf-8")
                templist.append(x[18].encode("utf-8"))
            else:
                templist.append(x[18].encode("utf-8"))
            if not isinstance(x[19], unicode):
                x[19] = unicode(x[19], "utf-8")
                templist.append(x[19].encode("utf-8"))
            else:
                templist.append(x[19].encode("utf-8"))
            if not isinstance(x[20], unicode):
                x[20] = unicode(x[20], "utf-8")
                templist.append(x[20].encode("utf-8"))
            else:
                templist.append(x[20].encode("utf-8"))
            if not isinstance(x[21], unicode):
                x[21] = unicode(x[21], "utf-8")
                templist.append(x[21].encode("utf-8"))
            else:
                templist.append(x[21].encode("utf-8"))
            if not isinstance(x[22], unicode):
                x[22] = unicode(x[22], "utf-8")
                templist.append(x[22].encode("utf-8"))
            else:
                templist.append(x[22].encode("utf-8"))
            if not isinstance(x[23], unicode):
                x[23] = unicode(x[23], "utf-8")
                templist.append(x[23].encode("utf-8"))
            else:
                templist.append(x[23].encode("utf-8"))
            if not isinstance(x[24], unicode):
                x[24] = unicode(x[24], "utf-8")
                templist.append(x[24].encode("utf-8"))
            else:
                templist.append(x[24].encode("utf-8"))
            if not isinstance(x[25], unicode):
                x[25] = unicode(x[25], "utf-8")
                templist.append(x[25].encode("utf-8"))
            else:
                templist.append(x[25].encode("utf-8"))
            if not isinstance(x[26], unicode):
                x[26] = unicode(x[26], "utf-8")
                templist.append(x[26].encode("utf-8"))
            else:
                templist.append(x[26].encode("utf-8"))
            if not isinstance(x[27], unicode):
                x[27] = unicode(x[27], "utf-8")
                templist.append(x[27].encode("utf-8"))
            else:
                templist.append(x[27].encode("utf-8"))
            if not isinstance(x[28], unicode):
                x[28] = unicode(x[28], "utf-8")
                templist.append(x[28].encode("utf-8"))
            else:
                templist.append(x[28].encode("utf-8"))
            if not isinstance(x[29], unicode):
                x[29] = unicode(x[29], "utf-8")
                templist.append(x[29].encode("utf-8"))
            else:
                templist.append(x[29].encode("utf-8"))
            if not isinstance(x[30], unicode):
                x[30] = unicode(x[30], "utf-8")
                templist.append(x[30].encode("utf-8"))
            else:
                templist.append(x[30].encode("utf-8"))
            return repr(templist)
    except Exception as e:
	    print(e)
	    print("-------------------------------------------------------")
	    print(templist)
	    return


            templist.append(x[0].encode("utf-8"))
            templist.append(x[1].encode("utf-8"))
            templist.append(x[2].encode("utf-8"))
            templist.append(x[3].encode("utf-8"))
            templist.append(x[4].encode("utf-8"))
            templist.append(x[5].encode("utf-8"))
            templist.append(x[6].encode("utf-8"))
            templist.append(x[7].encode("utf-8"))
            templist.append(x[8].encode("utf-8"))
            templist.append(x[9].encode("utf-8"))
            templist.append(x[10].encode("utf-8"))
            templist.append(x[11].encode("utf-8"))
            templist.append(x[12].encode("utf-8"))
            templist.append(x[13].encode("utf-8"))
            templist.append(x[14].encode("utf-8"))
            templist.append(x[15].encode("utf-8"))
            templist.append(x[16].encode("utf-8"))
            templist.append(x[17].encode("utf-8"))
            templist.append(x[18].encode("utf-8"))
            templist.append(x[19].encode("utf-8"))
            templist.append(x[20].encode("utf-8"))
            templist.append(x[21].encode("utf-8"))
            templist.append(x[22].encode("utf-8"))
            templist.append(x[23].encode("utf-8"))
            templist.append(x[24].encode("utf-8"))
            templist.append(x[25].encode("utf-8"))
            templist.append(x[26].encode("utf-8"))
            templist.append(x[27].encode("utf-8"))
            templist.append(x[28].encode("utf-8"))
            templist.append(x[29].encode("utf-8"))
            templist.append(x[30].encode("utf-8"))
			
df_fnl = df_fnl.withColumn("Proxy_Device_address", when(col("Proxy_Device_address").isNull(), "-").otherwise(col("Proxy_Device_address")))
df_fnl = df_fnl.withColumn("End_Date", when(col("End_Date").isNull(), "-").otherwise(col("End_Date")))
df_fnl = df_fnl.withColumn("End_Time", when(col("End_Time").isNull(), "-").otherwise(col("End_Time")))
df_fnl = df_fnl.withColumn("Device_custom_number2", when(col("Device_custom_number2").isNull(), "-").otherwise(col("Device_custom_number2")))
df_fnl = df_fnl.withColumn("Device_custom_number1", when(col("Device_custom_number1").isNull(), "-").otherwise(col("Device_custom_number1")))
df_fnl = df_fnl.withColumn("Source_Address", when(col("Source_Address").isNull(), "-").otherwise(col("Source_Address")))
df_fnl = df_fnl.withColumn("Source_Port", when(col("Source_Port").isNull(), "-").otherwise(col("Source_Port")))
df_fnl = df_fnl.withColumn("Target_Address", when(col("Target_Address").isNull(), "-").otherwise(col("Target_Address")))
df_fnl = df_fnl.withColumn("target_Port", when(col("target_Port").isNull(), "-").otherwise(col("target_Port")))
df_fnl = df_fnl.withColumn("Device_Severity", when(col("Device_Severity").isNull(), "-").otherwise(col("Device_Severity")))
df_fnl = df_fnl.withColumn("DeviceEvent_Class_ID", when(col("DeviceEvent_Class_ID").isNull(), "-").otherwise(col("DeviceEvent_Class_ID")))
df_fnl = df_fnl.withColumn("Bytes_In", when(col("Bytes_In").isNull(), "-").otherwise(col("Bytes_In")))
df_fnl = df_fnl.withColumn("Bytes_Out", when(col("Bytes_Out").isNull(), "-").otherwise(col("Bytes_Out")))
df_fnl = df_fnl.withColumn("Request_Method", when(col("Request_Method").isNull(), "-").otherwise(col("Request_Method")))
df_fnl = df_fnl.withColumn("Source_Application_Protocol", when(col("Source_Application_Protocol").isNull(), "-").otherwise(col("Source_Application_Protocol")))
df_fnl = df_fnl.withColumn("Destination_Host_Name", when(col("Destination_Host_Name").isNull(), "-").otherwise(col("Destination_Host_Name")))
df_fnl = df_fnl.withColumn("Destination_Application_Protocol", when(col("Destination_Application_Protocol").isNull(), "-").otherwise(col("Destination_Application_Protocol")))
df_fnl = df_fnl.withColumn("Destination_Port", when(col("Destination_Port").isNull(), "-").otherwise(col("Destination_Port")))
df_fnl = df_fnl.withColumn("Request_URL_file_name", when(col("Request_URL_file_name").isNull(), "-").otherwise(col("Request_URL_file_name")))
df_fnl = df_fnl.withColumn("Request_URL", when(col("Request_URL").isNull(), "-").otherwise(col("Request_URL")))
df_fnl = df_fnl.withColumn("Attacker_Username", when(col("Attacker_Username").isNull(), "-").otherwise(col("Attacker_Username")))
df_fnl = df_fnl.withColumn("Device_Custom_Strin1", when(col("Device_Custom_Strin1").isNull(), "-").otherwise(col("Device_Custom_Strin1")))
df_fnl = df_fnl.withColumn("Device_Host_Name", when(col("Device_Host_Name").isNull(), "-").otherwise(col("Device_Host_Name")))
df_fnl = df_fnl.withColumn("Target_Host_Name", when(col("Target_Host_Name").isNull(), "-").otherwise(col("Target_Host_Name")))
df_fnl = df_fnl.withColumn("Device_Custom_Strin4", when(col("Device_Custom_Strin4").isNull(), "-").otherwise(col("Device_Custom_Strin4")))
df_fnl = df_fnl.withColumn("Request_Context", when(col("Request_Context").isNull(), "-").otherwise(col("Request_Context")))
df_fnl = df_fnl.withColumn("Request_Client_Application", when(col("Request_Client_Application").isNull(), "-").otherwise(col("Request_Client_Application")))
df_fnl = df_fnl.withColumn("Device_Action", when(col("Device_Action").isNull(), "-").otherwise(col("Device_Action")))
df_fnl = df_fnl.withColumn("Device_Event_Category", when(col("Device_Event_Category").isNull(), "-").otherwise(col("Device_Event_Category")))
df_fnl = df_fnl.withColumn("Device_Custom_Strin3", when(col("Device_Custom_Strin3").isNull(), "-").otherwise(col("Device_Custom_Strin3")))
df_fnl = df_fnl.withColumn("Device_address", when(col("Device_address").isNull(), "-").otherwise(col("Device_address")))
df_fnl = df_fnl.withColumn("Manager_datetime", when(col("Manager_datetime").isNull(), "-").otherwise(col("Manager_datetime")))
df_fnl = df_fnl.withColumn("partdate", when(col("partdate").isNull(), "-").otherwise(col("partdate")))



newrdd = proxysample.mapPartitions(lambda line: csv.reader(line,delimiter='\s', quotechar='"')).filter(lambda line: len(line)>=3)




.filter(lambda x: len(x) > 3)

def splitter(line):
    try:
        if line != None: varlist = re.findall(r'[^"\s]\S*|".+?"', line)
        if len(varlist) > 3:
            return varlist[3:]
    except Exception as e:
	    print(line)


list_rdd = proxysample.map(splitter)



def iterate_rdd(x):
    try:
        if len(x) == 31:
            for x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29], x[30] in x[0:31]:
            for 			
                if isinstance(x[0], str):
                    x[0] = unicode(x[0], "utf-8")
                if isinstance(x[1], str):
                    x[1] = unicode(x[1], "utf-8")
                if isinstance(x[2], str):
                    x[2] = unicode(x[2], "utf-8")
                if isinstance(x[3], str):
                    x[3] = unicode(x[3], "utf-8")
                if isinstance(x[4], str):
                    x[4] = unicode(x[4], "utf-8")
                if isinstance(x[5], str):
                    x[5] = unicode(x[5], "utf-8")
                if isinstance(x[6], str):
                    x[6] = unicode(x[6], "utf-8")
                if isinstance(x[7], str):
                    x[7] = unicode(x[7], "utf-8")
                if isinstance(x[8], str):
                    x[8] = unicode(x[8], "utf-8")
                if isinstance(x[9], str):
                    x[9] = unicode(x[9], "utf-8")
                if isinstance(x[10], str):
                    x[10] = unicode(x[10], "utf-8")
                if isinstance(x[11], str):
                    x[11] = unicode(x[11], "utf-8")
                if isinstance(x[12], str):
                    x[12] = unicode(x[12], "utf-8")
                if isinstance(x[13], str):
                    x[13] = unicode(x[13], "utf-8")
                if isinstance(x[14], str):
                    x[14] = unicode(x[14], "utf-8")
                if isinstance(x[15], str):
                    x[15] = unicode(x[15], "utf-8")
                if isinstance(x[16], str):
                    x[16] = unicode(x[16], "utf-8")
                if isinstance(x[17], str):
                    x[17] = unicode(x[17], "utf-8")
                if isinstance(x[18], str):
                    x[18] = unicode(x[18], "utf-8")
                if isinstance(x[19], str):
                    x[19] = unicode(x[19], "utf-8")
                if isinstance(x[20], str):
                    x[20] = unicode(x[20], "utf-8")
                if isinstance(x[21], str):
                    x[21] = unicode(x[21], "utf-8")
                if isinstance(x[22], str):
                    x[22] = unicode(x[22], "utf-8")
                if isinstance(x[23], str):
                    x[23] = unicode(x[23], "utf-8")
                if isinstance(x[24], str):
                    x[24] = unicode(x[24], "utf-8")
                if isinstance(x[25], str):
                    x[25] = unicode(x[25], "utf-8")
                if isinstance(x[26], str):
                    x[26] = unicode(x[26], "utf-8")
                if isinstance(x[27], str):
                    x[27] = unicode(x[27], "utf-8")
                if isinstance(x[28], str):
                    x[28] = unicode(x[28], "utf-8")
                if isinstance(x[29], str):
                    x[29] = unicode(x[29], "utf-8")
                if isinstance(x[30], str):
                    x[30] = unicode(x[30], "utf-8")
					#str_list = repr(x[0].encode("utf-8")+'&#&'+x[1]+'&#&'+x[2]+'&#&'+x[3]+'&#&'+x[4]+'&#&'+x[5]+'&#&'+x[6]+'&#&'+x[7]+'&#&'+x[8]+'&#&'+x[9]+'&#&'+x[10]+'&#&'+x[11]+'&#&'+x[12]+'&#&'+x[13]+'&#&'+x[14]+'&#&'+x[15]+'&#&'+x[16]+'&#&'+x[17]+'&#&'+x[18]+'&#&'+x[19]+'&#&'+x[20]+'&#&'+x[21]+'&#&'+x[22]+'&#&'+x[23]+'&#&'+x[24]+'&#&'+x[25]+'&#&'+x[26]+'&#&'+x[27]+'&#&'+x[28]+'&#&'+x[29]+'&#&'+x[30]).split("&#&")
                    str_list = unicode(x[0].encode("utf-8")+'&#&'+x[1].encode("utf-8")+'&#&'+x[2].encode("utf-8")+'&#&'+x[3].encode("utf-8")+'&#&'+x[4].encode("utf-8")+'&#&'+x[5].encode("utf-8")+'&#&'+x[6].encode("utf-8")+'&#&'+x[7].encode("utf-8")+'&#&'+x[8].encode("utf-8")+'&#&'+x[9].encode("utf-8")+'&#&'+x[10].encode("utf-8")+'&#&'+x[11].encode("utf-8")+'&#&'+x[12].encode("utf-8")+'&#&'+x[13].encode("utf-8")+'&#&'+x[14].encode("utf-8")+'&#&'+x[15].encode("utf-8")+'&#&'+x[16].encode("utf-8")+'&#&'+x[17].encode("utf-8")+'&#&'+x[18].encode("utf-8")+'&#&'+x[19].encode("utf-8")+'&#&'+x[20].encode("utf-8")+'&#&'+x[21].encode("utf-8")+'&#&'+x[22].encode("utf-8")+'&#&'+x[23].encode("utf-8")+'&#&'+x[24].encode("utf-8")+'&#&'+x[25].encode("utf-8")+'&#&'+x[26].encode("utf-8")+'&#&'+x[27].encode("utf-8")+'&#&'+x[28].encode("utf-8")+'&#&'+x[29].encode("utf-8")+'&#&'+x[30].encode("utf-8")).split("&#&")
            return str_list
    except Exception as e:
	    print(e)
	    print("-------------------------------------------------------")
	    print(x)
	    return
