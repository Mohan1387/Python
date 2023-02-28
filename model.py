from __future__ import print_function
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import pyspark
import ast
from pyspark.sql import SparkSession
import sys
import json
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka010 import KafkaUtils
from pyspark.streaming.kafka010 import ConsumerStrategies
from pyspark.streaming.kafka010 import LocationStrategies

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

from pyspark.ml.feature import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.tuning import *
import pandas as pd
import numpy as np
import math
from collections import Counter
from datetime import datetime,timedelta
from tld import get_tld
from urllib.parse import urlparse
import string
import os
import csv
import warnings
import itertools


def get_domain(x):
    
    def domain_finder(link):
    
        dot_splitter = link.split('.')

        seperator_first = 0
        if '//' in dot_splitter[0]:
            seperator_first = (dot_splitter[0].find('//') + 2)

        seperator_end = ''
        for i in dot_splitter[2]:
            if i in string.punctuation:
                seperator_end = i
                break

        if seperator_end:
            end_ = dot_splitter[2].split(seperator_end)[0]
        else:
            end_ = dot_splitter[2]

        domain = [dot_splitter[0][seperator_first:], dot_splitter[1], end_]
        domain = '.'.join(domain)

        return domain

    try:
        res = urlparse(str(x)).netloc
        if res == '':
            return domain_finder(str(x))
        else:
            return res
    except:
        return x
    
    
def url_dots_count(x):
    return len(re.findall(r"\.", str(x)))


def url_dash_count(x):
    return len(re.findall(r"\-", str(x)))


def url_fslash_count(x):
    return len(re.findall(r"\/", str(x)))


def url_endsw_fslash(x):
    if str(x).endswith("/"):
        return 1
    else:
        return 0


def reg_char_count(x):
    
    pattern = r'[A-Za-z0-9/.-]+'
    reg_char = re.sub(pattern, '', str(x))
    return len(reg_char)


def ipin_url(x):
    match = re.search(
        '(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.'
        '([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\/)|'  # IPv4
        '(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.'
        '([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\/)|'  # IPv4 with port
        '((0x[0-9a-fA-F]{1,2})\\.(0x[0-9a-fA-F]{1,2})\\.(0x[0-9a-fA-F]{1,2})\\.(0x[0-9a-fA-F]{1,2})\\/)' # IPv4 in hexadecimal
        '(?:[a-fA-F0-9]{1,4}:){7}[a-fA-F0-9]{1,4}|'
        '([0-9]+(?:\.[0-9]+){3}:[0-9]+)|'
        '((?:(?:\d|[01]?\d\d|2[0-4]\d|25[0-5])\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d|\d)(?:\/\d{1,2})?)', str(x))  # Ipv6
    if match:
        return 1
    else:
        return 0


def has_suspicious_words(x):
    match = re.findall('paypal|login|signin|bank|account|update|free|lucky|service|bonus|ebayisapi|webscr|admin|server|client|contract|tender|payment|logon|signin|signup|service|end|renew|auth',
                       str(x).lower())
    if match:
        return 1
    else:
        return 0

    
def first_dir_len(x):
    urlpath= urlparse(str(x)).path
    try:
        return len(urlpath.split('/')[1])
    except:
        return 0
    

def domain_len(x):
    try:
        return len(str(x))
    except:
        return -1


# count of numbers in url
def url_digits_count(x):
    try:
        return len(''.join(filter(str.isdigit, str(x))))
    except:
        return -1


#Length of Top Level Domain
def topleveldomain_len(x):
    try:
        return len( get_tld(str(x), fix_protocol=True, fail_silently=True) )
    except:
        return -1


def relative_entropy(x, base=2):
    data = ''.join(str(x).split('.')[:-1])
    pattern = r'[^A-Za-z0-9-_]+'
    data = re.sub(pattern, '', data).lower()
    '''
    Calculate the relative entropy (Kullback-Leibler divergence) between data and expected values.
    '''
    entropy = 0.0
    length = len(data) * 1.0

    if length > 0:
        cnt = Counter(data)

        # These probability numbers were calculated from the Alexa Top
        # 1 million domains as of September 15th, 2017. TLDs and instances
        # of 'www' were removed so 'www.google.com' would be treated as
        # 'google' and 'images.google.com' would be 'images.google'.
        probabilities = {
            '-': 0.013342298553905901,
            '_': 9.04562613824129e-06,
            '0': 0.0024875471880163543,
            '1': 0.004884638114650296,
            '2': 0.004373560237839663,
            '3': 0.0021136613076357144,
            '4': 0.001625197496170685,
            '5': 0.0013070929769758662,
            '6': 0.0014880054997406921,
            '7': 0.001471421851820583,
            '8': 0.0012663876593537805,
            '9': 0.0010327089841158806,
            'a': 0.07333590631143488,
            'b': 0.04293204925644953,
            'c': 0.027385633133525503,
            'd': 0.02769469202658208,
            'e': 0.07086192756262588,
            'f': 0.01249653250998034,
            'g': 0.038516276096631406,
            'h': 0.024017645001386995,
            'i': 0.060447396668797414,
            'j': 0.007082725266242929,
            'k': 0.01659570875496002,
            'l': 0.05815885325582237,
            'm': 0.033884915513851865,
            'n': 0.04753175014774523,
            'o': 0.09413783122067709,
            'p': 0.042555148167356144,
            'q': 0.0017231917793349655,
            'r': 0.06460084667060655,
            's': 0.07214640647425614,
            't': 0.06447722311338391,
            'u': 0.034792493336388744,
            'v': 0.011637198026847418,
            'w': 0.013318176884203925,
            'x': 0.003170491961453572,
            'y': 0.016381628936354975,
            'z': 0.004715786426736459
        }
        for char, count in cnt.items():
            observed = count / length
            expected = probabilities[char]
            entropy += observed * math.log((observed / expected), base)
    return entropy


def url_endsw_file(x):
    x = str(x)
    if x.endswith(".aac"):
         return 1
    elif x.endswith(".adt"):
         return 1
    elif x.endswith(".adts"):
         return 1
    elif x.endswith(".accdb"):
         return 1
    elif x.endswith(".accde"):
         return 1
    elif x.endswith(".accdr"):
         return 1
    elif x.endswith(".accdt"):
         return 1
    elif x.endswith(".aif"):
         return 1
    elif x.endswith(".aifc"):
         return 1
    elif x.endswith(".aiff"):
         return 1
    elif x.endswith(".aspx"):
         return 1
    elif x.endswith(".avi"):
         return 1
    elif x.endswith(".bat"):
         return 1
    elif x.endswith(".bin"):
         return 1
    elif x.endswith(".bmp"):
         return 1
    elif x.endswith(".cab"):
         return 1
    elif x.endswith(".cda"):
         return 1
    elif x.endswith(".csv"):
         return 1
    elif x.endswith(".dif"):
         return 1
    elif x.endswith(".dll"):
         return 1
    elif x.endswith(".doc"):
         return 1
    elif x.endswith(".docm"):
         return 1
    elif x.endswith(".docx"):
         return 1
    elif x.endswith(".dot"):
         return 1
    elif x.endswith(".dotx"):
         return 1
    elif x.endswith(".eml"):
         return 1
    elif x.endswith(".eps"):
         return 1
    elif x.endswith(".exe"):
         return 1
    elif x.endswith(".flv"):
         return 1
    elif x.endswith(".gif"):
         return 1
    elif x.endswith(".ini"):
         return 1
    elif x.endswith(".iso"):
         return 1
    elif x.endswith(".jar"):
         return 1
    elif x.endswith(".jpg"):
         return 1
    elif x.endswith(".jpeg"):
         return 1
    elif x.endswith(".m4a"):
         return 1
    elif x.endswith(".mdb"):
         return 1
    elif x.endswith(".mid"):
         return 1
    elif x.endswith(".midi"):
         return 1
    elif x.endswith(".mov"):
         return 1
    elif x.endswith(".mp3"):
         return 1
    elif x.endswith(".mp4"):
         return 1
    elif x.endswith(".mpeg"):
         return 1
    elif x.endswith(".mpg"):
         return 1
    elif x.endswith(".msi"):
         return 1
    elif x.endswith(".mui"):
         return 1
    elif x.endswith(".pdf"):
         return 1
    elif x.endswith(".png"):
         return 1
    elif x.endswith(".pot"):
         return 1
    elif x.endswith(".potm"):
         return 1
    elif x.endswith(".potx"):
         return 1
    elif x.endswith(".ppam"):
         return 1
    elif x.endswith(".pps"):
         return 1
    elif x.endswith(".ppsm"):
         return 1
    elif x.endswith(".ppsx"):
         return 1
    elif x.endswith(".ppt"):
         return 1
    elif x.endswith(".pptm"):
         return 1
    elif x.endswith(".pptx"):
         return 1
    elif x.endswith(".psd"):
         return 1
    elif x.endswith(".pst"):
         return 1
    elif x.endswith(".pub"):
         return 1
    elif x.endswith(".rar"):
         return 1
    elif x.endswith(".rtf"):
         return 1
    elif x.endswith(".sldm"):
         return 1
    elif x.endswith(".sldx"):
         return 1
    elif x.endswith(".swf"):
         return 1
    elif x.endswith(".sys"):
         return 1
    elif x.endswith(".tif"):
         return 1
    elif x.endswith(".tiff"):
         return 1
    elif x.endswith(".tmp"):
         return 1
    elif x.endswith(".txt"):
         return 1
    elif x.endswith(".vob"):
         return 1
    elif x.endswith(".vsd"):
         return 1
    elif x.endswith(".vsdm"):
         return 1
    elif x.endswith(".vsdx"):
         return 1
    elif x.endswith(".vss"):
         return 1
    elif x.endswith(".vssm"):
         return 1
    elif x.endswith(".vst"):
         return 1
    elif x.endswith(".vstm"):
         return 1
    elif x.endswith(".vstx"):
         return 1
    elif x.endswith(".wav"):
         return 1
    elif x.endswith(".wbk"):
         return 1
    elif x.endswith(".wks"):
         return 1
    elif x.endswith(".wma"):
         return 1
    elif x.endswith(".wmd"):
         return 1
    elif x.endswith(".wmv"):
         return 1
    elif x.endswith(".wmz"):
         return 1
    elif x.endswith(".wms"):
         return 1
    elif x.endswith(".wpd"):
         return 1
    elif x.endswith(".wp5"):
         return 1
    elif x.endswith(".xla"):
         return 1
    elif x.endswith(".xlam"):
         return 1
    elif x.endswith(".xll"):
         return 1
    elif x.endswith(".xlm"):
         return 1
    elif x.endswith(".xls"):
         return 1
    elif x.endswith(".xlsm"):
         return 1
    elif x.endswith(".xlsx"):
         return 1
    elif x.endswith(".xlt"):
         return 1
    elif x.endswith(".xltm"):
         return 1
    elif x.endswith(".xltx"):
         return 1
    elif x.endswith(".xps"):
         return 1
    elif x.endswith(".zip"):
         return 1
    else:
        return 0


def has_uae_keywords(x):
    match = re.findall('abu dhabi|abu_dhabi|abudhabi|dubai|emirates|etihad|taqa|edge|mubadala|daman',
                       str(x).lower())
    if match:
        return 1
    else:
        return 0


spark = (SparkSession.builder.appName("Spark_Stream_Test_App")
         .config('spark.kafka.security.protocol', 'SASL_SSL')
         .config('spark.kafka.ssl.truststore.location', '/opt/cloudera/security/pki/cmtruststore.jks')
         .config('spark.kafka.ssl.truststore.password', 'password')
         .config("spark.executor.cores", "1")
         .config("spark.executor.memory",  "2g")
         .config("spark.driver.memory","6g")
         .config("spark.executor.instances", "3")
         .getOrCreate())


input_path = sys.argv[1]

df=spark.read.json(input_path)

raw_schema = StructType([
        StructField("future_use_0", StringType(), True),
        StructField("receive_time", StringType(), True),
        StructField("serial_number", StringType(), True),
        StructField("type", StringType(), True),
        StructField("threat_content_type", StringType(), True),
        StructField("future_use_1", StringType(), True),
        StructField("generated_time", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("destination_ip", StringType(), True),
        StructField("nat_source_ip", StringType(), True),
        StructField("nat_destination_ip", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("source_user", StringType(), True),
        StructField("destination_user", StringType(), True),
        StructField("application", StringType(), True),
        StructField("virtual_system", StringType(), True),
        StructField("source_zone", StringType(), True),
        StructField("destination_zone", StringType(), True),
        StructField("inbound_interface", StringType(), True),
        StructField("outbound_interface", StringType(), True),
        StructField("log_action", StringType(), True),
        StructField("future_use_2", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("repeat_count", StringType(), True),
        StructField("source_port", StringType(), True),
        StructField("destination_port", StringType(), True),
        StructField("nat_source_port", StringType(), True),
        StructField("nat_destination_port", StringType(), True),
        StructField("flags", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("action", StringType(), True),
        StructField("url_filename", StringType(), True),
        StructField("threat_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("sequence_number", StringType(), True),
        StructField("action_flags", StringType(), True),
        StructField("source_location", StringType(), True),
        StructField("destination_location", StringType(), True),
        StructField("future_use_3", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("pcap_id", StringType(), True),
        StructField("file_digest", StringType(), True),
        StructField("cloud", StringType(), True),
        StructField("url_index", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("file_type", StringType(), True),
        StructField("x_forwarded_for", StringType(), True),
        StructField("referer", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("recipient", StringType(), True),
        StructField("report_id", StringType(), True),
        StructField("device_group_hierarchy_level_1", StringType(), True),
        StructField("device_group_hierarchy_level_2", StringType(), True),
        StructField("device_group_hierarchy_level_3", StringType(), True),
        StructField("device_group_hierarchy_level_4", StringType(), True),
        StructField("virtual_system_name", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("future_use_4", StringType(), True),
        StructField("source_vm_uuid", StringType(), True),
        StructField("destination_vm_uuid", StringType(), True),
        StructField("http_method", StringType(), True),
        StructField("tunnel_id_imsi", StringType(), True),
        StructField("monitor_tag_imei", StringType(), True),
        StructField("parent_session_id", StringType(), True),
        StructField("parent_start_time", StringType(), True),
        StructField("tunnel_type", StringType(), True),
        StructField("threat_category", StringType(), True),
        StructField("content_version", StringType(), True),
        StructField("future_use_5", StringType(), True),
        StructField("sctp_association_id", StringType(), True),
        StructField("payload_protocol_id", StringType(), True),
        StructField("http_headers", StringType(), True)
        ])


df1 = spark.read.option("quote", "\"").option("escape", "\"").csv(df.rdd.map(lambda x: x[8]), schema=raw_schema)

df1 =df1.withColumn("date_", regexp_replace(col("receive_time"), '/', '-') )
df1 = df1.withColumn("date_",col("date_").cast("Timestamp"))
df1 = df1.withColumn("date_",from_utc_timestamp(col("date_"),"UTC"))
df1 = df1.withColumn("partdate_",to_date(col("date_")))
df1 = df1.withColumn("partdate_",col("partdate_").cast(StringType()))

# Create a Data Validation Check url field and filter before applying the functions
df1 = df1.filter(col('action').isin(['alert', 'block-url'])).filter(col('threat_content_type') == 'url').filter(col('url_filename').isNotNull())
df1 = df1.select(['url_filename','partdate_'])


feature_schema = StructType([
    StructField("partdate_", StringType(), False),
    StructField("url", StringType(), False),
    StructField("url_dots_count", IntegerType(), False),
    StructField("url_dash_count", IntegerType(), False),
    StructField("url_fslash_count", IntegerType(), False),
    StructField("url_endsw_fslash", IntegerType(), False),
    StructField("reg_char_count", IntegerType(), False),
    StructField("ipin_url", IntegerType(), False),
    StructField("has_suspicious_words", IntegerType(), False),
    StructField("first_dir_len", IntegerType(), False),
    StructField("domain_len", IntegerType(), False),
    StructField("url_digits_count", IntegerType(), False),
    StructField("topleveldomain_len", IntegerType(), False),
    StructField("relative_entropy", FloatType(), False),
    StructField("url_endsw_file", IntegerType(), False),
    StructField("has_uae_keywords", IntegerType(), False)
])


def create_features(x):

    d =  get_domain(x[1])
    x1 =  url_dots_count(x[1])
    x2 =  url_dash_count(x[1])
    x3 =  url_fslash_count(x[1])
    x4 =  url_endsw_fslash(x[1])
    x5 =  reg_char_count(x[1])
    x6 =  ipin_url(x[1])
    x7 =  has_suspicious_words(x[1])
    x8 =  first_dir_len(x[1])
    x9 =  domain_len(d)
    x10 =  url_digits_count(x[1])
    x11 =  topleveldomain_len(x[1])
    x12 =  relative_entropy(d)
    x13 =  url_endsw_file(x[1])
    x14 =  has_uae_keywords(x[1])

    return (x[0],x[1],x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14)


create_features_udf = udf(create_features, feature_schema)

df1 = df1.select(create_features_udf(struct("partdate_","url_filename")).alias('url_filename'))

df1 = df1.select("url_filename.partdate_","url_filename.url",
                  "url_filename.url_dots_count",
                  "url_filename.url_dash_count",
                  "url_filename.url_fslash_count",
                  "url_filename.url_endsw_fslash",
                  "url_filename.reg_char_count",
                  "url_filename.ipin_url",
                  "url_filename.has_suspicious_words",
                  "url_filename.first_dir_len",
                  "url_filename.domain_len",
                  "url_filename.url_digits_count",
                  "url_filename.topleveldomain_len",
                  "url_filename.relative_entropy",
                  "url_filename.url_endsw_file",
                  "url_filename.has_uae_keywords")
                  
                  
from sklearn.feature_extraction.text import HashingVectorizer
from joblib import dump, load
import pickle
from io import BytesIO
from xgboost import XGBClassifier
import xgboost as xgb
from pyspark.mllib.linalg import Vectors, VectorUDT


hash_model = spark.sparkContext.binaryFiles("/user/mmanivannan/mlmodel/mal_url/hashvector_model/HashVec.pkl")
hvectorizer = hash_model.collect()

hvectorizer_model = spark.sparkContext.broadcast(hvectorizer[0][1])

hash_schema=StructType([
    StructField("hash_0", IntegerType(), False),
    StructField("hash_1", IntegerType(), False),
    StructField("hash_2", IntegerType(), False),
    StructField("hash_3", IntegerType(), False),
    StructField("hash_4", IntegerType(), False),
    StructField("hash_5", IntegerType(), False),
    StructField("hash_6", IntegerType(), False),
    StructField("hash_7", IntegerType(), False),
    StructField("hash_8", IntegerType(), False),
    StructField("hash_9", IntegerType(), False),
    StructField("hash_10", IntegerType(), False),
    StructField("hash_11", IntegerType(), False),
    StructField("hash_12", IntegerType(), False),
    StructField("hash_13", IntegerType(), False),
    StructField("hash_14", IntegerType(), False),
    StructField("hash_15", IntegerType(), False),
    StructField("hash_16", IntegerType(), False),
    StructField("hash_17", IntegerType(), False),
    StructField("hash_18", IntegerType(), False),
    StructField("hash_19", IntegerType(), False)
])


def get_hash_feature(x):

    return tuple(load(BytesIO(hvectorizer_model.value)).transform([x]).toarray().reshape(-1).tolist())

get_hash_feature_udf = udf(get_hash_feature, hash_schema)


df1 = df1.withColumn('hash_feature', get_hash_feature_udf(col("url")))


df1 = df1.select(['partdate_','url','url_dots_count','url_dash_count','url_fslash_count','url_endsw_fslash',
'reg_char_count','ipin_url','has_suspicious_words','first_dir_len','domain_len',
'url_digits_count','topleveldomain_len','relative_entropy','url_endsw_file',
'has_uae_keywords','hash_feature.hash_0','hash_feature.hash_1','hash_feature.hash_2',
'hash_feature.hash_3','hash_feature.hash_4','hash_feature.hash_5','hash_feature.hash_6',
'hash_feature.hash_7','hash_feature.hash_8','hash_feature.hash_9','hash_feature.hash_10',
'hash_feature.hash_11','hash_feature.hash_12','hash_feature.hash_13','hash_feature.hash_14',
'hash_feature.hash_15','hash_feature.hash_16','hash_feature.hash_17','hash_feature.hash_18',
'hash_feature.hash_19'])


model_rdd_pkl = spark.sparkContext.binaryFiles("/user/mmanivannan/mlmodel/mal_url/xgboost_model/XgbClassifier")
model_rdd_data = model_rdd_pkl.collect()
broadcast_mal_url_model = spark.sparkContext.broadcast(model_rdd_data[0][1])


def classify_event(x):
    
    pred = load(BytesIO(broadcast_mal_url_model.value)).predict(np.array(x).reshape((1,34)))
    
    return int(pred[0])

classify_event_udf = udf(classify_event, IntegerType())


df1 = df1.withColumn('Prediction', classify_event_udf(struct('url_dots_count','url_dash_count','url_fslash_count','url_endsw_fslash',
'reg_char_count','ipin_url','has_suspicious_words','first_dir_len','domain_len',
'url_digits_count','topleveldomain_len','relative_entropy','url_endsw_file',
'has_uae_keywords','hash_0','hash_1','hash_2','hash_3','hash_4','hash_5','hash_6',
'hash_7','hash_8','hash_9','hash_10','hash_11','hash_12','hash_13','hash_14',
'hash_15','hash_16','hash_17','hash_18','hash_19')))


df1 = df1.select(['url','url_dots_count','url_dash_count','url_fslash_count','url_endsw_fslash',
'reg_char_count','ipin_url','has_suspicious_words','first_dir_len','domain_len',
'url_digits_count','topleveldomain_len','relative_entropy','url_endsw_file',
'has_uae_keywords','hash_0','hash_1','hash_2','hash_3','hash_4','hash_5','hash_6',
'hash_7','hash_8','hash_9','hash_10','hash_11','hash_12','hash_13','hash_14',
'hash_15','hash_16','hash_17','hash_18','hash_19', 'Prediction', 'partdate_'])


df1.write.mode("append").format('ORC')..partitionBy("partdate_").saveAsTable("edge_db.ml_mal_url")


#dvar=$(date -d "-1 days"  +%Y%m%d)
#spark-submit  --master yarn  model.py hdfs://master:54310/user/hdfs/OWA/compressed/$dvar/*.snappy
