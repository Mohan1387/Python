__author__ = 'Mohan Manivannan' 
__name__ = 'Application to add difficulty level in making a Beef recipe' 
 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *

import re
import sys
 
#function to split time from cooktime and perptime columns and convert hours to minutes  
def total_time(a): 
     
    # PT from the string 
    line = a[2:] 
     
    #pattern search with regex to extract numbers 
    searchObj1 = re.search( r'^(\d*)H(\d*)M$', line) 
    searchObj2 = re.search( r'^(\d*)H$', line) 
    searchObj3 = re.search( r'^(\d*)M$', line) 
 
    #calculating cumulative minutes     
    if searchObj1: 
      total_mins = searchObj1.group(1) 
      total_mins = int(total_mins) 
      total_mins = total_mins*60 
      mins = searchObj1.group(2) 
      mins = int(mins) 
      total_mins = total_mins+mins 
      return total_mins 
    elif searchObj2: 
      total_mins = searchObj2.group(1) 
      total_mins = int(total_mins) 
      total_mins = total_mins*60 
      return total_mins 
    elif searchObj3: 
      total_mins = searchObj3.group(1) 
      total_mins = int(total_mins) 
      return total_mins 
    else: 
      return 0 
 
def main(sc, inputfile, outputfile): 
 
    sqlcontext = SQLContext(sc)     
     
    Recipes_df = sqlcontext.read.json(inputfile) 
    #Recipes_df.show() 
    Recipes_df.registerTempTable('Recepie_table') 
     
    #filtering beef related recepie 
    filtered_recepie = sqlcontext.sql("select cookTime,datePublished,description,image,ingredients,name,prepTime,recipeYield, url from Recepie_table where lower(ingredients) like '%beef%' ") 
     
    #creating udf  
    udfcook_mins = udf(total_time, StringType()) 
     
    #applying udf to convert hours to miuntes in both cookTime and perpTime     
    filtered_recepie_one = filtered_recepie.withColumn("cooktime_mins",udfcook_mins("cookTime")) 
    filtered_recepie_one = filtered_recepie_one.withColumn("preptime_mins",udfcook_mins("prepTime")) 
 
    #calcutae total time and classifying difficulties  
    filtered_recepie_one.registerTempTable('Recepie_table_total_time') 
     
    #SQL Used in the process 
    '''select a.cookTime, 
    a.datePublished, 
    a.description, 
    a.image, 
    a.ingredients, 
    a.name, 
    a.prepTime, 
    a.recipeYield, 
    a.url, 
    case when a.total_time >= 60 then "Hard"  
    when a.total_time < 60 and a.total_time >= 30 then "Medium" 
    when a.total_time < 30 then "Easy" 
    else "Unknown" End as difficulty 
    from (select cookTime, 
        datePublished, 
        description, 
        image, 
        ingredients, 
        name, 
        prepTime, 
        recipeYield, 
        url, 
        (cooktime_mins + preptime_mins) as totaltime  
        from Recepie_table_total_time) a ''' 
 
    recepie_total_time = sqlcontext.sql("select a.cookTime,a.datePublished,a.description,a.image,a.ingredients,a.name,a.prepTime,a.recipeYield,a.url,case when a.total_time >= 60 then 'Hard' when a.total_time < 60 and a.total_time >= 30 then 'Medium' when a.total_time < 30 then 'Easy' else 'Unknown' End as difficulty from (select cookTime,datePublished,description,image,ingredients,name,prepTime,recipeYield,url,(cooktime_mins + preptime_mins) as total_time from Recepie_table_total_time) a") 
 
    #output the result set as a parquet file 
    recepie_total_time.write.parquet(outputfile) 
     
if __name__ == "__main__": 
 
    #spark configuration 
    conf = SparkConf().setAppName("recepie_diff")
    #conf = conf.setMaster("local[2]")
    sc = SparkContext(conf=conf) 
    #input and output file      
    inputfile = sys.argv[1] 
    outputfile = sys.argv[2] 
 
    #Execute Main function 
    main(sc, inputfile, outputfile)
   
   #spark-submit --master yarn-cluster RecepieEtl.py /user/hdfs/test/hellofresh/recipes.json /user/hdfs/test/hellofresh/recepie_with_diff
