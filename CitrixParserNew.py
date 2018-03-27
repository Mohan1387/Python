from pyspark.sql.functions import col,split
import re
import datetime
import collections as cl
from pyspark.sql.functions import col,split
import re
import datetime
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import HiveContext

inputfile = sys.argv[1]
dateval = sys.argv[2]

#read CEF format AD windows log from HDFS
#adsample = sc.textFile("hdfs://master.one.du.com/user/hdfs/adsample/citrix.txt")
adsample = sc.textFile(inputfile)

#Define base list and insert all the lines into the list
baselist = []

for line in adsample.toLocalIterator():
    baselist.append(line.strip())

#get Date value to get present day's year value
getdate = datetime.datetime.now() #- datetime.timedelta(days = 1)#enable to get yesterday year used on Dec 31 log parsed on Jan 1st
#pdate = datetime.datetime.now().strftime('%Y%m%d')
partDate = dateval

#declare a main list to capture all the fields
main_list = []

#iterate over each line from base list and conver each line to list of string. and append the list in a main list 
for a in baselist:
    
    checkvar = re.split('\s+(?=\w+=)',a)
    newlist = checkvar[0].split("|")
    eventdate = newlist[0][:6]+' '+str(getdate.year)+' '+newlist[0][7:15]
    
    newlist[0] =  'eventdate='+eventdate
    newlist[2] = 'product='+newlist[2]
    newlist[4] = 'logType='+newlist[4]
    newlist[5] = 'eventType='+newlist[5]
    
    checkvar.pop(0)
    
    checkvar = [newlist[4]]+checkvar
    checkvar = [newlist[5]]+checkvar
    checkvar = [newlist[2]]+checkvar
    checkvar = [newlist[0]]+checkvar
    #checkvar = partDate+checkvar
    
    main_list.append(checkvar)

    checkvar = 'null'
    newlist = 'null'

#main_list

#declare a list to store a list of list into list of dictionary
outter_list = []

for ol in main_list:
    mydict = {}
    for il in ol:
        #inner_list = 'null'
        dict_var = re.split('(\w+)=',il)
        dict_var.pop(0)
        #dict_var
        mydict[dict_var[0]] = dict_var[1]
        #inner_list.append(mydict)
    outter_list.append(mydict)
    

write_list = []
    
#iterate over the list of Dict to assign the values to the list of variable. and use the variable to seperate key from value    
for ol in outter_list:
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
    
    for k,v in ol.items():
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
	        suser = v
        elif k == 'type':
	        type_ = v
        else:
            continue
    #print(logType+'&&'+eventdate)
    #print(act+'&&'+agentDnsDomain+'&&'+agentNtDomain+'&&'+agentTranslatedAddress+'&&'+agentTranslatedZoneExternalID+'&&'+agentTranslatedZoneURI+'&&'+agentZoneExternalID+'&&'+agentZoneURI+'&&'+agt+'&&'+ahost+'&&'+aid+'&&'+amac+'&&'+app+'&&'+art+'&&'+at+'&&'+atz+'&&'+av+'&&'+c6a1+'&&'+c6a1Label+'&&'+c6a3+'&&'+c6a3Label+'&&'+c6a4+'&&'+C6a4Label+'&&'+cat+'&&'+cfp1+'&&'+cfp1Label+'&&'+cfp2+'&&'+cfp2Label+'&&'+cfp3+'&&'+cfp3Label+'&&'+cfp4+'&&'+cfp4Label+'&&'+cn1+'&&'+cn1Label+'&&'+cn2+'&&'+cn2Label+'&&'+cn3+'&&'+cn3Label+'&&'+cnt+'&&'+cs1+'&&'+cs1Label+'&&'+cs2+'&&'+cs2Label+'&&'+cs3+'&&'+cs3Label+'&&'+cs4+'&&'+cs4Label+'&&'+cs5+'&&'+cs5Label+'&&'+cs6+'&&'+cs6Label+'&&'+customerExternalID+'&&'+customerURI+'&&'+destinationDnsDomain+'&&'+destinationServiceName+'&&'+destinationTranslatedZoneURI+'&&'+destinationTranslatedAddress+'&&'+destinationTranslatedPort+'&&'+destinationTranslatedZoneExternalID+'&&'+destinationZoneURI+'&&'+destinationZoneExternalID+'&&'+deviceCustomDate1+'&&'+deviceCustomDate1Label+'&&'+deviceCustomDate2+'&&'+deviceCustomDate2Label+'&&'+deviceDirection+'&&'+deviceDnsDomain+'&&'+deviceExternalId+'&&'+deviceFacility+'&&'+deviceInboundInterface+'&&'+deviceNtDomain+'&&'+DeviceOutboundInterface+'&&'+DevicePayloadId+'&&'+deviceProcessName+'&&'+deviceTranslatedAddress+'&&'+deviceTranslatedZoneURI+'&&'+deviceTranslatedZoneExternalID+'&&'+deviceZoneExternalID+'&&'+deviceZoneURI+'&&'+dhost+'&&'+dlat+'&&'+dlong+'&&'+dmac+'&&'+dntdom+'&&'+dpid+'&&'+dpriv+'&&'+dproc+'&&'+dpt+'&&'+dst+'&&'+dtz+'&&'+duid+'&&'+duser+'&&'+dvc+'&&'+dvchost+'&&'+dvcmac+'&&'+dvcpid+'&&'+end+'&&'+eventdate+'&&'+eventId+'&&'+eventType+'&&'+externalId+'&&'+fileCreateTime+'&&'+fileHash+'&&'+fileId+'&&'+fileModificationTime+'&&'+filePath+'&&'+filePermission+'&&'+fileType+'&&'+flexDate1+'&&'+flexDate1Label+'&&'+flexString1+'&&'+flexString1Label+'&&'+flexString2+'&&'+flexString2Label+'&&'+fname+'&&'+fsize+'&&'+in_+'&&'+logType+'&&'+msg+'&&'+oldFileCreateTime+'&&'+oldFileHash+'&&'+oldFileId+'&&'+oldFileModificationTime+'&&'+oldFileName+'&&'+oldFilePath+'&&'+oldFilePermission+'&&'+oldFileSize+'&&'+oldFileType+'&&'+out+'&&'+outcome+'&&'+product+'&&'+proto+'&&'+rawEvent+'&&'+reason+'&&'+request+'&&'+requestClientApplication+'&&'+requestContext+'&&'+requestCookies+'&&'+requestMethod+'&&'+rt+'&&'+shost+'&&'+slat+'&&'+slong+'&&'+smac+'&&'+sntdom+'&&'+sourceDnsDomain+'&&'+sourceServiceName+'&&'+sourceTranslatedAddress+'&&'+sourceTranslatedPort+'&&'+sourceTranslatedZoneURI+'&&'+sourceTranslatedZoneExternalID+'&&'+sourceZoneExternalID+'&&'+sourceZoneURI+'&&'+spid+'&&'+spriv+'&&'+sproc+'&&'+spt+'&&'+src+'&&'+start+'&&'+suid+'&&'+suser+'&&'+type_+'&&'+partDate)
    #convert the vaiurbale into list of string that contains values of each key
    str_list = str(act+'&&'+agentDnsDomain+'&&'+agentNtDomain+'&&'+agentTranslatedAddress+'&&'+agentTranslatedZoneExternalID+'&&'+agentTranslatedZoneURI+'&&'+agentZoneExternalID+'&&'+agentZoneURI+'&&'+agt+'&&'+ahost+'&&'+aid+'&&'+amac+'&&'+app+'&&'+art+'&&'+at+'&&'+atz+'&&'+av+'&&'+c6a1+'&&'+c6a1Label+'&&'+c6a3+'&&'+c6a3Label+'&&'+c6a4+'&&'+C6a4Label+'&&'+cat+'&&'+cfp1+'&&'+cfp1Label+'&&'+cfp2+'&&'+cfp2Label+'&&'+cfp3+'&&'+cfp3Label+'&&'+cfp4+'&&'+cfp4Label+'&&'+cn1+'&&'+cn1Label+'&&'+cn2+'&&'+cn2Label+'&&'+cn3+'&&'+cn3Label+'&&'+cnt+'&&'+cs1+'&&'+cs1Label+'&&'+cs2+'&&'+cs2Label+'&&'+cs3+'&&'+cs3Label+'&&'+cs4+'&&'+cs4Label+'&&'+cs5+'&&'+cs5Label+'&&'+cs6+'&&'+cs6Label+'&&'+customerExternalID+'&&'+customerURI+'&&'+destinationDnsDomain+'&&'+destinationServiceName+'&&'+destinationTranslatedZoneURI+'&&'+destinationTranslatedAddress+'&&'+destinationTranslatedPort+'&&'+destinationTranslatedZoneExternalID+'&&'+destinationZoneURI+'&&'+destinationZoneExternalID+'&&'+deviceCustomDate1+'&&'+deviceCustomDate1Label+'&&'+deviceCustomDate2+'&&'+deviceCustomDate2Label+'&&'+deviceDirection+'&&'+deviceDnsDomain+'&&'+deviceExternalId+'&&'+deviceFacility+'&&'+deviceInboundInterface+'&&'+deviceNtDomain+'&&'+DeviceOutboundInterface+'&&'+DevicePayloadId+'&&'+deviceProcessName+'&&'+deviceTranslatedAddress+'&&'+deviceTranslatedZoneURI+'&&'+deviceTranslatedZoneExternalID+'&&'+deviceZoneExternalID+'&&'+deviceZoneURI+'&&'+dhost+'&&'+dlat+'&&'+dlong+'&&'+dmac+'&&'+dntdom+'&&'+dpid+'&&'+dpriv+'&&'+dproc+'&&'+dpt+'&&'+dst+'&&'+dtz+'&&'+duid+'&&'+duser+'&&'+dvc+'&&'+dvchost+'&&'+dvcmac+'&&'+dvcpid+'&&'+end+'&&'+eventdate+'&&'+eventId+'&&'+eventType+'&&'+externalId+'&&'+fileCreateTime+'&&'+fileHash+'&&'+fileId+'&&'+fileModificationTime+'&&'+filePath+'&&'+filePermission+'&&'+fileType+'&&'+flexDate1+'&&'+flexDate1Label+'&&'+flexString1+'&&'+flexString1Label+'&&'+flexString2+'&&'+flexString2Label+'&&'+fname+'&&'+fsize+'&&'+in_+'&&'+logType+'&&'+msg+'&&'+oldFileCreateTime+'&&'+oldFileHash+'&&'+oldFileId+'&&'+oldFileModificationTime+'&&'+oldFileName+'&&'+oldFilePath+'&&'+oldFilePermission+'&&'+oldFileSize+'&&'+oldFileType+'&&'+out+'&&'+outcome+'&&'+product+'&&'+proto+'&&'+rawEvent+'&&'+reason+'&&'+request+'&&'+requestClientApplication+'&&'+requestContext+'&&'+requestCookies+'&&'+requestMethod+'&&'+rt+'&&'+shost+'&&'+slat+'&&'+slong+'&&'+smac+'&&'+sntdom+'&&'+sourceDnsDomain+'&&'+sourceServiceName+'&&'+sourceTranslatedAddress+'&&'+sourceTranslatedPort+'&&'+sourceTranslatedZoneURI+'&&'+sourceTranslatedZoneExternalID+'&&'+sourceZoneExternalID+'&&'+sourceZoneURI+'&&'+spid+'&&'+spriv+'&&'+sproc+'&&'+spt+'&&'+src+'&&'+start+'&&'+suid+'&&'+suser+'&&'+type_+'&&'+partDate).split("&&")
    write_list.append(str_list)

#convert the list of list into a Data frame
from pyspark.sql.types import *		

#define schema of the Data frame
table_schema = StructType([StructField("act", StringType(), True), StructField("agentDnsDomain", StringType(), True), StructField("agentNtDomain", StringType(), True), StructField("agentTranslatedAddress", StringType(), True), StructField("agentTranslatedZoneExternalID", StringType(), True), StructField("agentTranslatedZoneURI", StringType(), True), StructField("agentZoneExternalID", StringType(), True), StructField("agentZoneURI", StringType(), True), StructField("agt", StringType(), True), StructField("ahost", StringType(), True), StructField("aid", StringType(), True), StructField("amac", StringType(), True), StructField("app", StringType(), True), StructField("art", StringType(), True), StructField("at", StringType(), True), StructField("atz", StringType(), True), StructField("av", StringType(), True), StructField("c6a1  ", StringType(), True), StructField("c6a1Label", StringType(), True), StructField("c6a3", StringType(), True), StructField("c6a3Label", StringType(), True), StructField("c6a4", StringType(), True), StructField("C6a4Label", StringType(), True), StructField("cat", StringType(), True), StructField("cfp1", StringType(), True), StructField("cfp1Label", StringType(), True), StructField("cfp2", StringType(), True), StructField("cfp2Label", StringType(), True), StructField("cfp3", StringType(), True), StructField("cfp3Label", StringType(), True), StructField("cfp4", StringType(), True), StructField("cfp4Label", StringType(), True), StructField("cn1", StringType(), True), StructField("cn1Label", StringType(), True), StructField("cn2", StringType(), True), StructField("cn2Label", StringType(), True), StructField("cn3", StringType(), True), StructField("cn3Label", StringType(), True), StructField("cnt", StringType(), True), StructField("cs1", StringType(), True), StructField("cs1Label", StringType(), True), StructField("cs2", StringType(), True), StructField("cs2Label", StringType(), True), StructField("cs3", StringType(), True), StructField("cs3Label", StringType(), True), StructField("cs4", StringType(), True), StructField("cs4Label", StringType(), True), StructField("cs5", StringType(), True), StructField("cs5Label", StringType(), True), StructField("cs6", StringType(), True), StructField("cs6Label", StringType(), True), StructField("customerExternalID", StringType(), True), StructField("customerURI", StringType(), True), StructField("destinationDnsDomain", StringType(), True), StructField("destinationServiceName", StringType(), True), StructField("destinationTranslatedZoneURI", StringType(), True), StructField("destinationTranslatedAddress", StringType(), True), StructField("destinationTranslatedPort", StringType(), True), StructField("destinationTranslatedZoneExternalID", StringType(), True), StructField("destinationZoneURI", StringType(), True), StructField("destinationZoneExternalID", StringType(), True), StructField("deviceCustomDate1", StringType(), True), StructField("deviceCustomDate1Label", StringType(), True), StructField("deviceCustomDate2", StringType(), True), StructField("deviceCustomDate2Label", StringType(), True), StructField("deviceDirection", StringType(), True), StructField("deviceDnsDomain", StringType(), True), StructField("deviceExternalId", StringType(), True), StructField("deviceFacility", StringType(), True), StructField("deviceInboundInterface", StringType(), True), StructField("deviceNtDomain", StringType(), True), StructField("DeviceOutboundInterface", StringType(), True), StructField("DevicePayloadId", StringType(), True), StructField("deviceProcessName", StringType(), True), StructField("deviceTranslatedAddress", StringType(), True), StructField("deviceTranslatedZoneURI", StringType(), True), StructField("deviceTranslatedZoneExternalID", StringType(), True), StructField("deviceZoneExternalID", StringType(), True), StructField("deviceZoneURI", StringType(), True), StructField("dhost", StringType(), True), StructField("dlat", StringType(), True), StructField("dlong", StringType(), True), StructField("dmac", StringType(), True), StructField("dntdom", StringType(), True), StructField("dpid", StringType(), True), StructField("dpriv", StringType(), True), StructField("dproc", StringType(), True), StructField("dpt", StringType(), True), StructField("dst", StringType(), True), StructField("dtz", StringType(), True), StructField("duid", StringType(), True), StructField("duser", StringType(), True), StructField("dvc", StringType(), True), StructField("dvchost", StringType(), True), StructField("dvcmac", StringType(), True), StructField("dvcpid", StringType(), True), StructField("end", StringType(), True), StructField("eventdate", StringType(), True), StructField("eventId", StringType(), True), StructField("eventType", StringType(), True), StructField("externalId", StringType(), True), StructField("fileCreateTime", StringType(), True), StructField("fileHash", StringType(), True), StructField("fileId", StringType(), True), StructField("fileModificationTime", StringType(), True), StructField("filePath", StringType(), True), StructField("filePermission", StringType(), True), StructField("fileType", StringType(), True), StructField("flexDate1", StringType(), True), StructField("flexDate1Label", StringType(), True), StructField("flexString1", StringType(), True), StructField("flexString1Label", StringType(), True), StructField("flexString2", StringType(), True), StructField("flexString2Label", StringType(), True), StructField("fname", StringType(), True), StructField("fsize", StringType(), True), StructField("in_", StringType(), True), StructField("logType", StringType(), True), StructField("msg", StringType(), True), StructField("oldFileCreateTime", StringType(), True), StructField("oldFileHash", StringType(), True), StructField("oldFileId", StringType(), True), StructField("oldFileModificationTime", StringType(), True), StructField("oldFileName", StringType(), True), StructField("oldFilePath", StringType(), True), StructField("oldFilePermission", StringType(), True), StructField("oldFileSize", StringType(), True), StructField("oldFileType", StringType(), True), StructField("out", StringType(), True), StructField("outcome", StringType(), True), StructField("product", StringType(), True), StructField("proto", StringType(), True), StructField("rawEvent", StringType(), True), StructField("reason", StringType(), True), StructField("request", StringType(), True), StructField("requestClientApplication", StringType(), True), StructField("requestContext", StringType(), True), StructField("requestCookies", StringType(), True), StructField("requestMethod", StringType(), True), StructField("rt", StringType(), True), StructField("shost", StringType(), True), StructField("slat", StringType(), True), StructField("slong", StringType(), True), StructField("smac", StringType(), True), StructField("sntdom", StringType(), True), StructField("sourceDnsDomain", StringType(), True), StructField("sourceServiceName", StringType(), True), StructField("sourceTranslatedAddress", StringType(), True), StructField("sourceTranslatedPort", StringType(), True), StructField("sourceTranslatedZoneURI", StringType(), True), StructField("sourceTranslatedZoneExternalID", StringType(), True), StructField("sourceZoneExternalID", StringType(), True), StructField("sourceZoneURI", StringType(), True), StructField("spid", StringType(), True), StructField("spriv", StringType(), True), StructField("sproc", StringType(), True), StructField("spt", StringType(), True), StructField("src", StringType(), True), StructField("start", StringType(), True), StructField("suid", StringType(), True), StructField("suser", StringType(), True), StructField("type_", StringType(), True), StructField("partDate", StringType(), True)])

#to convert the list of list to Data Frame with schema
df = spark.createDataFrame(write_list,schema=table_schema)

#inserting the DataFame to a Existing hive table. create the table with the same schema in hive before this step
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
df.registerTempTable('tab_name')
hive_context.sql("SET hive.exec.compress.output=true")
hive_context.sql("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
hive_context.sql("SET mapred.output.compression.type=BLOCK")
hive_context.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
hive_context.sql("SET hive.exec.dynamic.partition=true")
hive_context.sql("insert into table masterlogdb.citrix select * from tab_name")
