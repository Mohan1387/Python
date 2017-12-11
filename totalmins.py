import re
def total_time(a):
 line = a[2:]
 searchObj1 = re.search( r'^(\d*)H(\d*)M$', line) 
 searchObj2 = re.search( r'^(\d*)H$', line) 
 searchObj3 = re.search( r'^(\d*)M$', line) 
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
def main(): 
 #PT15M  
 #PT1H 
 a = 'PT' 
 print('--------------') 
 print(total_time(a)) 
 print('--------------') 
if __name__ == "__main__":  
 main()
