#__author__ = 'Mohan Manivannan' 
#__name__ = 'Application to parse Citrix logs'
#__Version__ = '1.0'
#Create on 8th Feb 2018 Version 1.0
#updated on 26 march 2017 changed all processing logic to RDD

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

#spark configuration 
#conf = SparkConf().setAppName("CitrixlogParser")
#sc = SparkContext(conf=conf)
#sqlcontext = SQLContext(sc)
#hive_context = HiveContext(sc)
#hive_context.setConf("hive.metastore.uris", "thrift://master:9083")
#SparkContext.setSystemProperty("hive.metastore.uris", "thrift://master:9083")
#spark = (SparkSession.builder.appName('CitrixlogParser').enableHiveSupport().getOrCreate())
spark = (SparkSession.builder.appName("OWALogParser").config("SPARK_WORKER_MEMORY","6g").config("spark.executor.cores","2").config("spark.driver.memory", "6g").config("spark.executor.memory", "6g").config("hive.metastore.uris", "thrift://master:9083").config("spark.sql.hive.metastore.version", "2.1.1").config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").config("spark.sql.hive.metastore.sharedPrefixes", "com.mysql.jdbc").config("spark.sql.warehouse.dir", "hdfs://master:54310/user/hive/warehouse").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.compress.output", "true").config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec").config("mapred.output.compression.type", "BLOCK").enableHiveSupport().getOrCreate())


#we can also define conf to sparksession at run time by below. set new runtime options
#spark.conf.set("spark.sql.shuffle.partitions", 6)
#spark.conf.set("spark.executor.memory", "2g")

#input file and current date
inputfile = sys.argv[1]
dateval = str(sys.argv[2])
#read CEF format AD windows log from HDFS
adsample = spark.sparkContext.textFile(inputfile)
#adsample = spark.read.text(inputfile)

#Define base list rdd and insert all the lines into the rdd
pattern = re.compile('\s+(?=\w+=)')
list_rdd = adsample.map(lambda x: pattern.split(x))

#get Date value to get present day's year value
getdate = datetime.datetime.now() #- datetime.timedelta(days = 1)#enable to get yesterday year used on Dec 31 log parsed on Jan 1st
indtyear = str(getdate.year)
#paritition variable to make partition based on date
partDate = dateval

#define function and iterate over each line from base list rdd  and conver each line to list of string.
def f(x):
    newlist = 'null'
    checklist = 'null'
    checklist = []
    newlist = x[0].split("|")
    eventdate = newlist[0][12:19]+' '+str(getdate.year)+' '+newlist[0][19:28]

    checklist.append('eventdate='+eventdate)
    checklist.append('product='+newlist[2])
    checklist.append('logType='+newlist[4])
    checklist.append('eventType='+newlist[5])
    x.pop(0)
    return x+(checklist)

res = list_rdd.map(f)

#define function and apply to  convert list of list rdd into list of dictionary rdd
def g(a):
    outter_list = 'null'
    outter_list = []
    mydict = {}
    for i in range(0, len(a)):
        dict_var = re.split('(\w+)=', a[i])
        dict_var.pop(0)
        mydict[dict_var[0]] = dict_var[1]
    return mydict

dist_res = res.map(g)
  
#iterate over the list of Dict to assign the values to the list of variable. and use the variable to seperate key from value
def fres(y):
    act = '-'
    agentDnsDomain = '-'
    agentNtDomain = '-'
    agentTranslatedAddress = '-'
    agentTranslatedZoneExternalID = '-'
    agentTranslatedZoneURI = '-'
    agentZoneExternalID = '-'
    agentZoneURI = '-'
    agt = '-'
    ahost = '-'
    aid = '-'
    amac = '-'
    app = '-'
    art = '-'
    at = '-'
    atz = '-'
    av = '-'
    c6a1 = '-'  
    c6a1Label = '-'
    c6a3 = '-'
    c6a3Label = '-'
    c6a4 = '-'
    C6a4Label = '-'
    cat = '-'
    cfp1 = '-'
    cfp1Label = '-'
    cfp2 = '-'
    cfp2Label = '-'
    cfp3 = '-'
    cfp3Label = '-'
    cfp4 = '-'
    cfp4Label = '-'
    cn1 = '-'
    cn1Label = '-'
    cn2 = '-'
    cn2Label = '-'
    cn3 = '-'
    cn3Label = '-'
    cnt = '-'
    cs1 = '-'
    cs1Label = '-'
    cs2 = '-'
    cs2Label = '-'
    cs3 = '-'
    cs3Label = '-'
    cs4 = '-'
    cs4Label = '-'
    cs5 = '-'
    cs5Label = '-'
    cs6 = '-'
    cs6Label = '-'
    customerExternalID = '-'
    customerURI = '-'
    destinationDnsDomain = '-'
    destinationServiceName = '-'
    destinationTranslatedZoneURI = '-'
    destinationTranslatedAddress = '-'
    destinationTranslatedPort = '-'
    destinationTranslatedZoneExternalID = '-'
    destinationZoneURI = '-'
    destinationZoneExternalID = '-'
    deviceCustomDate1 = '-'
    deviceCustomDate1Label = '-'
    deviceCustomDate2 = '-'
    deviceCustomDate2Label = '-'
    deviceDirection = '-'
    deviceDnsDomain = '-'
    deviceExternalId = '-'
    deviceFacility = '-'
    deviceInboundInterface = '-'
    deviceNtDomain = '-'
    DeviceOutboundInterface = '-'
    DevicePayloadId = '-'
    deviceProcessName = '-'
    deviceTranslatedAddress = '-'
    deviceTranslatedZoneURI = '-'
    deviceTranslatedZoneExternalID = '-'
    deviceZoneExternalID = '-'
    deviceZoneURI = '-'
    dhost = '-'
    dlat = '-'
    dlong = '-'
    dmac = '-'
    dntdom = '-'
    dpid = '-'
    dpriv = '-'
    dproc = '-'
    dpt = '-'
    dst = '-'
    dtz = '-'
    duid = '-'
    duser = '-'
    dvc = '-'
    dvchost = '-'
    dvcmac = '-'
    dvcpid = '-'
    end = '-'
    eventdate = '-'
    eventId = '-'
    eventType = '-'
    externalId = '-'
    fileCreateTime = '-'
    fileHash = '-'
    fileId = '-'
    fileModificationTime = '-'
    filePath = '-'
    filePermission = '-'
    fileType = '-'
    flexDate1 = '-'
    flexDate1Label = '-'
    flexString1 = '-'
    flexString1Label = '-'
    flexString2 = '-'
    flexString2Label = '-'
    fname = '-'
    fsize = '-'
    in_ = '-'
    logType = '-'
    msg = '-'
    oldFileCreateTime = '-'
    oldFileHash = '-'
    oldFileId = '-'
    oldFileModificationTime = '-'
    oldFileName = '-'
    oldFilePath = '-'
    oldFilePermission = '-'
    oldFileSize = '-'
    oldFileType = '-'
    out = '-'
    outcome = '-'
    product = '-'
    proto = '-'
    rawEvent = '-'
    reason = '-'
    request = '-'
    requestClientApplication = '-'
    requestContext = '-'
    requestCookies = '-'
    requestMethod = '-'
    rt = '-'
    shost = '-'
    slat = '-'
    slong = '-'
    smac = '-'
    sntdom = '-'
    sourceDnsDomain = '-'
    sourceServiceName = '-'
    sourceTranslatedAddress = '-'
    sourceTranslatedPort = '-'
    sourceTranslatedZoneURI = '-'
    sourceTranslatedZoneExternalID = '-'
    sourceZoneExternalID = '-'
    sourceZoneURI = '-'
    spid = '-'
    spriv = '-'
    sproc = '-'
    spt = '-'
    src = '-'
    start = '-'
    suid = '-'
    suser = '-'
    type_ = '-'
    for k,v in y.items():
        if k == 'act':
            act = v
        elif k == 'agentDnsDomain':
            agentDnsDomain = v
        elif k == 'agentNtDomain':
            agentNtDomain = v
        elif k == 'agentTranslatedAddress':
            agentTranslatedAddress = v
        elif k == 'agentTranslatedZoneExternalID':
            agentTranslatedZoneExternalID = v
        elif k == 'agentTranslatedZoneURI':
            agentTranslatedZoneURI = v
        elif k == 'agentZoneExternalID':
            agentZoneExternalID = v
        elif k == 'agentZoneURI':
            agentZoneURI = v
        elif k == 'agt':
            agt = v
        elif k == 'ahost':
            ahost = v
        elif k == 'aid':
            aid = v
        elif k == 'amac':
            amac = v
        elif k == 'app':
            app = v
        elif k == 'art':
            art = v
        elif k == 'at':
            at = v
        elif k == 'atz':
            atz = v
        elif k == 'av':
            av = v
        elif k == 'c6a1':
            c6a1 = v
        elif k == 'c6a1Label':
            c6a1Label = v
        elif k == 'c6a3':
            c6a3 = v
        elif k == 'c6a3Label':
            c6a3Label = v
        elif k == 'c6a4':
            c6a4 = v
        elif k == 'C6a4Label':
            C6a4Label = v
        elif k == 'cat':
            cat = v
        elif k == 'cfp1':
            cfp1 = v
        elif k == 'cfp1Label':
            cfp1Label = v
        elif k == 'cfp2':
            cfp2 = v
        elif k == 'cfp2Label':
            cfp2Label = v
        elif k == 'cfp3':
            cfp3 = v
        elif k == 'cfp3Label':
            cfp3Label = v
        elif k == 'cfp4':
            cfp4 = v
        elif k == 'cfp4Label':
            cfp4Label = v
        elif k == 'cn1':
            cn1 = v
        elif k == 'cn1Label':
            cn1Label = v
        elif k == 'cn2':
            cn2 = v
        elif k == 'cn2Label':
            cn2Label = v
        elif k == 'cn3':
            cn3 = v
        elif k == 'cn3Label':
            cn3Label = v
        elif k == 'cnt':
            cnt = v
        elif k == 'cs1':
            cs1 = v
        elif k == 'cs1Label':
            cs1Label = v
        elif k == 'cs2':
            cs2 = v
        elif k == 'cs2Label':
            cs2Label = v
        elif k == 'cs3':
            cs3 = v
        elif k == 'cs3Label':
            cs3Label = v
        elif k == 'cs4':
            cs4 = v
        elif k == 'cs4Label':
            cs4Label = v
        elif k == 'cs5':
            cs5 = v
        elif k == 'cs5Label':
            cs5Label = v
        elif k == 'cs6':
            cs6 = v
        elif k == 'cs6Label':
            cs6Label = v
        elif k == 'customerExternalID':
            customerExternalID = v
        elif k == 'customerURI':
            customerURI = v
        elif k == 'destinationDnsDomain':
            destinationDnsDomain = v
        elif k == 'destinationServiceName':
            destinationServiceName = v
        elif k == 'destinationTranslatedZoneURI':
            destinationTranslatedZoneURI = v
        elif k == 'destinationTranslatedAddress':
            destinationTranslatedAddress = v
        elif k == 'destinationTranslatedPort':
            destinationTranslatedPort = v
        elif k == 'destinationTranslatedZoneExternalID':
            destinationTranslatedZoneExternalID = v
        elif k == 'destinationZoneURI':
            destinationZoneURI = v
        elif k == 'destinationZoneExternalID':
            destinationZoneExternalID = v
        elif k == 'deviceCustomDate1':
            deviceCustomDate1 = v
        elif k == 'deviceCustomDate1Label':
            deviceCustomDate1Label = v
        elif k == 'deviceCustomDate2':
            deviceCustomDate2 = v
        elif k == 'deviceCustomDate2Label':
            deviceCustomDate2Label = v
        elif k == 'deviceDirection':
            deviceDirection = v
        elif k == 'deviceDnsDomain':
            deviceDnsDomain = v
        elif k == 'deviceExternalId':
            deviceExternalId = v
        elif k == 'deviceFacility':
            deviceFacility = v
        elif k == 'deviceInboundInterface':
            deviceInboundInterface = v
        elif k == 'deviceNtDomain':
            deviceNtDomain = v
        elif k == 'DeviceOutboundInterface':
            DeviceOutboundInterface = v
        elif k == 'DevicePayloadId':
            DevicePayloadId = v
        elif k == 'deviceProcessName':
            deviceProcessName = v
        elif k == 'deviceTranslatedAddress':
            deviceTranslatedAddress = v
        elif k == 'deviceTranslatedZoneURI':
            deviceTranslatedZoneURI = v
        elif k == 'deviceTranslatedZoneExternalID':
            deviceTranslatedZoneExternalID = v
        elif k == 'deviceZoneExternalID':
            deviceZoneExternalID = v
        elif k == 'deviceZoneURI':
            deviceZoneURI = v
        elif k == 'dhost':
            dhost = v
        elif k == 'dlat':
            dlat = v
        elif k == 'dlong':
            dlong = v
        elif k == 'dmac':
            dmac = v
        elif k == 'dntdom':
            dntdom = v
        elif k == 'dpid':
            dpid = v
        elif k == 'dpriv':
            dpriv = v
        elif k == 'dproc':
            dproc = v
        elif k == 'dpt':
            dpt = v
        elif k == 'dst':
            dst = v
        elif k == 'dtz':
            dtz = v
        elif k == 'duid':
            duid = v
        elif k == 'duser':
            duser = v
        elif k == 'dvc':
            dvc = v
        elif k == 'dvchost':
            dvchost = v
        elif k == 'dvcmac':
            dvcmac = v
        elif k == 'dvcpid':
            dvcpid = v
        elif k == 'end':
            end= v
        elif k == 'eventdate':
            eventdate = v
        elif k == 'eventId':
            eventId = v
        elif k == 'eventType':
            eventType = v
        elif k == 'externalId':
            externalId = v
        elif k == 'fileCreateTime':
            fileCreateTime = v
        elif k == 'fileHash':
            fileHash = v
        elif k == 'fileId':
            fileId = v
        elif k == 'fileModificationTime':
            fileModificationTime = v
        elif k == 'filePath':
            filePath = v
        elif k == 'filePermission':
            filePermission = v
        elif k == 'fileType':
            fileType = v
        elif k == 'flexDate1':
            flexDate1 = v
        elif k == 'flexDate1Label':
            flexDate1Label = v
        elif k == 'flexString1':
            flexString1 = v
        elif k == 'flexString1Label':
            flexString1Label = v
        elif k == 'flexString2':
            flexString2 = v
        elif k == 'flexString2Label':
            flexString2Label = v
        elif k == 'fname':
            fname = v
        elif k == 'fsize':
            fsize = v
        elif k == 'in':
            in_ = v
        elif k == 'logType':
            logType = v
        elif k == 'msg':
            msg = v
        elif k == 'oldFileCreateTime':
            oldFileCreateTime = v
        elif k == 'oldFileHash':
            oldFileHash = v
        elif k == 'oldFileId':
            oldFileId = v
        elif k == 'oldFileModificationTime':
            oldFileModificationTime = v
        elif k == 'oldFileName':
            oldFileName = v
        elif k == 'oldFilePath':
            oldFilePath = v
        elif k == 'oldFilePermission':
            oldFilePermission = v
        elif k == 'oldFileSize':
            oldFileSize = v
        elif k == 'oldFileType':
            oldFileType = v
        elif k == 'out':
            out = v
        elif k == 'outcome':
            outcome = v
        elif k == 'product':
            product = v
        elif k == 'proto':
            proto = v
        elif k == 'rawEvent':
            rawEvent = v
        elif k == 'reason':
            reason = v
        elif k == 'request':
            request = v
        elif k == 'requestClientApplication':
            requestClientApplication = v
        elif k == 'requestContext':
            requestContext = v
        elif k == 'requestCookies':
            requestCookies = v
        elif k == 'requestMethod':
            requestMethod = v
        elif k == 'rt':
            rt = v
        elif k == 'shost':
            shost = v
        elif k == 'slat':
            slat = v
        elif k == 'slong':
            slong = v
        elif k == 'smac':
            smac = v
        elif k == 'sntdom':
            sntdom = v
        elif k == 'sourceDnsDomain':
            sourceDnsDomain = v
        elif k == 'sourceServiceName':
            sourceServiceName = v
        elif k == 'sourceTranslatedAddress':
            sourceTranslatedAddress = v
        elif k == 'sourceTranslatedPort':
            sourceTranslatedPort = v
        elif k == 'sourceTranslatedZoneURI':
            sourceTranslatedZoneURI = v
        elif k == 'sourceTranslatedZoneExternalID':
            sourceTranslatedZoneExternalID = v
        elif k == 'sourceZoneExternalID':
            sourceZoneExternalID = v
        elif k == 'sourceZoneURI':
            sourceZoneURI = v
        elif k == 'spid':
            spid = v
        elif k == 'spriv':
            spriv = v
        elif k == 'sproc':
            sproc = v
        elif k == 'spt':
            spt = v
        elif k == 'src':
            src = v
        elif k == 'start':
            start = v
        elif k == 'suid':
            suid = v
        elif k == 'suser':
            suser = v.lower().replace('\\\\', '').replace('ducorp', '')
        elif k == 'type':
            type_ = v
        else:
            continue
    #print(eventdate+'---------------------------------------------------')
    #convert the vaiurbale into list of string that contains values of each key
    str_list = repr(act+'&&'+agentDnsDomain+'&&'+agentNtDomain+'&&'+agentTranslatedAddress+'&&'+agentTranslatedZoneExternalID+'&&'+agentTranslatedZoneURI+'&&'+agentZoneExternalID+'&&'+agentZoneURI+'&&'+agt+'&&'+ahost+'&&'+aid+'&&'+amac+'&&'+app+'&&'+art+'&&'+at+'&&'+atz+'&&'+av+'&&'+c6a1+'&&'+c6a1Label+'&&'+c6a3+'&&'+c6a3Label+'&&'+c6a4+'&&'+C6a4Label+'&&'+cat+'&&'+cfp1+'&&'+cfp1Label+'&&'+cfp2+'&&'+cfp2Label+'&&'+cfp3+'&&'+cfp3Label+'&&'+cfp4+'&&'+cfp4Label+'&&'+cn1+'&&'+cn1Label+'&&'+cn2+'&&'+cn2Label+'&&'+cn3+'&&'+cn3Label+'&&'+cnt+'&&'+cs1+'&&'+cs1Label+'&&'+cs2+'&&'+cs2Label+'&&'+cs3+'&&'+cs3Label+'&&'+cs4+'&&'+cs4Label+'&&'+cs5+'&&'+cs5Label+'&&'+cs6+'&&'+cs6Label+'&&'+customerExternalID+'&&'+customerURI+'&&'+destinationDnsDomain+'&&'+destinationServiceName+'&&'+destinationTranslatedZoneURI+'&&'+destinationTranslatedAddress+'&&'+destinationTranslatedPort+'&&'+destinationTranslatedZoneExternalID+'&&'+destinationZoneURI+'&&'+destinationZoneExternalID+'&&'+deviceCustomDate1+'&&'+deviceCustomDate1Label+'&&'+deviceCustomDate2+'&&'+deviceCustomDate2Label+'&&'+deviceDirection+'&&'+deviceDnsDomain+'&&'+deviceExternalId+'&&'+deviceFacility+'&&'+deviceInboundInterface+'&&'+deviceNtDomain+'&&'+DeviceOutboundInterface+'&&'+DevicePayloadId+'&&'+deviceProcessName+'&&'+deviceTranslatedAddress+'&&'+deviceTranslatedZoneURI+'&&'+deviceTranslatedZoneExternalID+'&&'+deviceZoneExternalID+'&&'+deviceZoneURI+'&&'+dhost+'&&'+dlat+'&&'+dlong+'&&'+dmac+'&&'+dntdom+'&&'+dpid+'&&'+dpriv+'&&'+dproc+'&&'+dpt+'&&'+dst+'&&'+dtz+'&&'+duid+'&&'+duser+'&&'+dvc+'&&'+dvchost+'&&'+dvcmac+'&&'+dvcpid+'&&'+end+'&&'+eventdate+'&&'+eventId+'&&'+eventType+'&&'+externalId+'&&'+fileCreateTime+'&&'+fileHash+'&&'+fileId+'&&'+fileModificationTime+'&&'+filePath+'&&'+filePermission+'&&'+fileType+'&&'+flexDate1+'&&'+flexDate1Label+'&&'+flexString1+'&&'+flexString1Label+'&&'+flexString2+'&&'+flexString2Label+'&&'+fname+'&&'+fsize+'&&'+in_+'&&'+logType+'&&'+msg+'&&'+oldFileCreateTime+'&&'+oldFileHash+'&&'+oldFileId+'&&'+oldFileModificationTime+'&&'+oldFileName+'&&'+oldFilePath+'&&'+oldFilePermission+'&&'+oldFileSize+'&&'+oldFileType+'&&'+out+'&&'+outcome+'&&'+product+'&&'+proto+'&&'+rawEvent+'&&'+reason+'&&'+request+'&&'+requestClientApplication+'&&'+requestContext+'&&'+requestCookies+'&&'+requestMethod+'&&'+rt+'&&'+shost+'&&'+slat+'&&'+slong+'&&'+smac+'&&'+sntdom+'&&'+sourceDnsDomain+'&&'+sourceServiceName+'&&'+sourceTranslatedAddress+'&&'+sourceTranslatedPort+'&&'+sourceTranslatedZoneURI+'&&'+sourceTranslatedZoneExternalID+'&&'+sourceZoneExternalID+'&&'+sourceZoneURI+'&&'+spid+'&&'+spriv+'&&'+sproc+'&&'+spt+'&&'+src+'&&'+start+'&&'+suid+'&&'+suser+'&&'+type_+'&&'+str(partDate)).split('&&')
    #write_list.append(str_list)
    return str_list

res_dataframe = dist_res.map(fres)

#convert the list of list rdd to a Data frame
#define schema of the Data frame
table_schema = StructType([StructField("act", StringType(), True), StructField("agentDnsDomain", StringType(), True), StructField("agentNtDomain", StringType(), True), StructField("agentTranslatedAddress", StringType(), True), StructField("agentTranslatedZoneExternalID", StringType(), True), StructField("agentTranslatedZoneURI", StringType(), True), StructField("agentZoneExternalID", StringType(), True), StructField("agentZoneURI", StringType(), True), StructField("agt", StringType(), True), StructField("ahost", StringType(), True), StructField("aid", StringType(), True), StructField("amac", StringType(), True), StructField("app", StringType(), True), StructField("art", StringType(), True), StructField("at", StringType(), True), StructField("atz", StringType(), True), StructField("av", StringType(), True), StructField("c6a1  ", StringType(), True), StructField("c6a1Label", StringType(), True), StructField("c6a3", StringType(), True), StructField("c6a3Label", StringType(), True), StructField("c6a4", StringType(), True), StructField("C6a4Label", StringType(), True), StructField("cat", StringType(), True), StructField("cfp1", StringType(), True), StructField("cfp1Label", StringType(), True), StructField("cfp2", StringType(), True), StructField("cfp2Label", StringType(), True), StructField("cfp3", StringType(), True), StructField("cfp3Label", StringType(), True), StructField("cfp4", StringType(), True), StructField("cfp4Label", StringType(), True), StructField("cn1", StringType(), True), StructField("cn1Label", StringType(), True), StructField("cn2", StringType(), True), StructField("cn2Label", StringType(), True), StructField("cn3", StringType(), True), StructField("cn3Label", StringType(), True), StructField("cnt", StringType(), True), StructField("cs1", StringType(), True), StructField("cs1Label", StringType(), True), StructField("cs2", StringType(), True), StructField("cs2Label", StringType(), True), StructField("cs3", StringType(), True), StructField("cs3Label", StringType(), True), StructField("cs4", StringType(), True), StructField("cs4Label", StringType(), True), StructField("cs5", StringType(), True), StructField("cs5Label", StringType(), True), StructField("cs6", StringType(), True), StructField("cs6Label", StringType(), True), StructField("customerExternalID", StringType(), True), StructField("customerURI", StringType(), True), StructField("destinationDnsDomain", StringType(), True), StructField("destinationServiceName", StringType(), True), StructField("destinationTranslatedZoneURI", StringType(), True), StructField("destinationTranslatedAddress", StringType(), True), StructField("destinationTranslatedPort", StringType(), True), StructField("destinationTranslatedZoneExternalID", StringType(), True), StructField("destinationZoneURI", StringType(), True), StructField("destinationZoneExternalID", StringType(), True), StructField("deviceCustomDate1", StringType(), True), StructField("deviceCustomDate1Label", StringType(), True), StructField("deviceCustomDate2", StringType(), True), StructField("deviceCustomDate2Label", StringType(), True), StructField("deviceDirection", StringType(), True), StructField("deviceDnsDomain", StringType(), True), StructField("deviceExternalId", StringType(), True), StructField("deviceFacility", StringType(), True), StructField("deviceInboundInterface", StringType(), True), StructField("deviceNtDomain", StringType(), True), StructField("DeviceOutboundInterface", StringType(), True), StructField("DevicePayloadId", StringType(), True), StructField("deviceProcessName", StringType(), True), StructField("deviceTranslatedAddress", StringType(), True), StructField("deviceTranslatedZoneURI", StringType(), True), StructField("deviceTranslatedZoneExternalID", StringType(), True), StructField("deviceZoneExternalID", StringType(), True), StructField("deviceZoneURI", StringType(), True), StructField("dhost", StringType(), True), StructField("dlat", StringType(), True), StructField("dlong", StringType(), True), StructField("dmac", StringType(), True), StructField("dntdom", StringType(), True), StructField("dpid", StringType(), True), StructField("dpriv", StringType(), True), StructField("dproc", StringType(), True), StructField("dpt", StringType(), True), StructField("dst", StringType(), True), StructField("dtz", StringType(), True), StructField("duid", StringType(), True), StructField("duser", StringType(), True), StructField("dvc", StringType(), True), StructField("dvchost", StringType(), True), StructField("dvcmac", StringType(), True), StructField("dvcpid", StringType(), True), StructField("end", StringType(), True), StructField("eventdate", StringType(), True), StructField("eventId", StringType(), True), StructField("eventType", StringType(), True), StructField("externalId", StringType(), True), StructField("fileCreateTime", StringType(), True), StructField("fileHash", StringType(), True), StructField("fileId", StringType(), True), StructField("fileModificationTime", StringType(), True), StructField("filePath", StringType(), True), StructField("filePermission", StringType(), True), StructField("fileType", StringType(), True), StructField("flexDate1", StringType(), True), StructField("flexDate1Label", StringType(), True), StructField("flexString1", StringType(), True), StructField("flexString1Label", StringType(), True), StructField("flexString2", StringType(), True), StructField("flexString2Label", StringType(), True), StructField("fname", StringType(), True), StructField("fsize", StringType(), True), StructField("in_", StringType(), True), StructField("logType", StringType(), True), StructField("msg", StringType(), True), StructField("oldFileCreateTime", StringType(), True), StructField("oldFileHash", StringType(), True), StructField("oldFileId", StringType(), True), StructField("oldFileModificationTime", StringType(), True), StructField("oldFileName", StringType(), True), StructField("oldFilePath", StringType(), True), StructField("oldFilePermission", StringType(), True), StructField("oldFileSize", StringType(), True), StructField("oldFileType", StringType(), True), StructField("out", StringType(), True), StructField("outcome", StringType(), True), StructField("product", StringType(), True), StructField("proto", StringType(), True), StructField("rawEvent", StringType(), True), StructField("reason", StringType(), True), StructField("request", StringType(), True), StructField("requestClientApplication", StringType(), True), StructField("requestContext", StringType(), True), StructField("requestCookies", StringType(), True), StructField("requestMethod", StringType(), True), StructField("rt", StringType(), True), StructField("shost", StringType(), True), StructField("slat", StringType(), True), StructField("slong", StringType(), True), StructField("smac", StringType(), True), StructField("sntdom", StringType(), True), StructField("sourceDnsDomain", StringType(), True), StructField("sourceServiceName", StringType(), True), StructField("sourceTranslatedAddress", StringType(), True), StructField("sourceTranslatedPort", StringType(), True), StructField("sourceTranslatedZoneURI", StringType(), True), StructField("sourceTranslatedZoneExternalID", StringType(), True), StructField("sourceZoneExternalID", StringType(), True), StructField("sourceZoneURI", StringType(), True), StructField("spid", StringType(), True), StructField("spriv", StringType(), True), StructField("sproc", StringType(), True), StructField("spt", StringType(), True), StructField("src", StringType(), True), StructField("start", StringType(), True), StructField("suid", StringType(), True), StructField("suser", StringType(), True), StructField("type_", StringType(), True), StructField("partDate", StringType(), True)])
#to convert the list of list to Data Frame with schema
df_fnl = spark.createDataFrame(res_dataframe.map(lambda x: x),schema=table_schema)
df_fnl = df_fnl.withColumn("partDate", regexp_replace("partDate", ".$" , ""))
#df_fnl = df_fnl.withColumn("suser", lower(col("suser"))
#df_fnl = df_fnl.withColumn("suser", regexp_replace("suser", "\\\\", "")) 
#df = df.select(col("partDate"), substring_index(col("partDate"), "\'", 1).as("partDate"))
#hive_context = HiveContext(sc)
df_fnl.registerTempTable('tab_name')
#hive_context.sql("SET hive.exec.compress.output=true")
#hive_context.sql("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
#hive_context.sql("SET mapred.output.compression.type=BLOCK")
#hive_context.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
#hive_context.sql("SET hive.exec.dynamic.partition=true")
spark.sql("insert into table masterlogdb.owa select * from tab_name")

#dvar=$(date -d "-1 days"  +%Y%m%d)
#spark-submit  --master yarn --executor-cores 3  --num-executors 8 --driver-memory 10g /home/hduser/pyspark_script/OWAParser.py hdfs://master:54310/user/hdfs/OWA/compressed/$dvar/*.snappy $dvar
