def add(var,var1):
 
 e = {"What's 2 added by 2?":4,"What's 3 added by 3?":6,"What's 10 added by 10?":20}
 i = {"What's 13 added by 7?":20,"What's 12 added by 9?":21,"What's 16 added by 6?":22}
 h = {"What's 43 added by 27?":70,"What's 27 added by 47?":74,"What's 29 added by 37?":66}
 
 if var == "easy" :
  ind = 1
  for key,value in e.items():
     
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1
 if var == "intermediate" :    
  ind = 1
  for key,value in i.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

 if var == "hard" :    
  ind = 1
  for key,value in h.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

def sub(var,var1):
 
 e = {"What's 2 subracted by 2?":0,"What's 3 subracted by 2?":1,"What's 10 subracted by 5?":5}
 i = {"What's 13 subracted by 7?":6,"What's 12 subracted by 9?":3,"What's 16 subracted by 6?":10}
 h = {"What's 43 subracted by 27?":16,"What's 47 subracted by 17?":30,"What's 37 subracted by 29?":8}
 
 if var == "easy" :
  ind = 1
  for key,value in e.items():
     
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1
 if var == "intermediate" :    
  ind = 1
  for key,value in i.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

 if var == "hard" :    
  ind = 1
  for key,value in h.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

def mul(var,var1):
 
 e = {"What's 2 multiplied by 2?":4,"What's 3 multiplied by 2?":6,"What's 10 multiplied by 5?":50}
 i = {"What's 13 multiplied by 7?":81,"What's 12 multiplied by 9?":108,"What's 16 multiplied by 6?":96}
 h = {"What's 43 multiplied by 27?":1161,"What's 47 multiplied by 17?":799,"What's 37 multiplied by 29?":1073}
 
 if var == "easy" :
  ind = 1
  for key,value in e.items():
     
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1
 if var == "intermediate" :    
  ind = 1
  for key,value in i.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

 if var == "hard" :    
  ind = 1
  for key,value in h.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

def div(var,var1):
 
 e = {"What's 2 divided by 2?":1,"What's 6 divided by 2?":3,"What's 10 divided by 5?":2}
 i = {"What's 100 divided by 5?":20,"What's 120 divided by 10?":12,"What's 120 divided by 6?":20}
 h = {"What's 6300 divided by 450?":14,"What's 3250 divided by 250?":13,"What's 3600 divided by 300?":12}
 
 if var == "easy" :
  ind = 1
  for key,value in e.items():
     
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1
 if var == "intermediate" :    
  ind = 1
  for key,value in i.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

 if var == "hard" :    
  ind = 1
  for key,value in h.items():
      
      if ind <= var1 :
        q1 = raw_input(key)
        q1 = int(q1)
        if q1 == value :
         print "That's right Well done!!!!"
        else:
         print "Wrong. Answer is: "+str(value)
      ind=ind+1

def level(ne):
 var = str(raw_input(ne))
 
 if (var == "easy"):
    print 1
    j = 0
    while(j < j+1):
     j = 0    
     ad = raw_input("Please give us the number of question you want to attempt highest is 3:")
     var1 = int(ad)
    
     if var1 <= 3 and var1 > 0 :
         print ad
         k = 0
         while(k < k+1):
             k = 0
             prob1 = raw_input("Specify the question type (multiplication:M, addition:A, subtraction:S, division:D) :")
             prob1 = str(prob1)
             if prob1 == "M" :
                 mul(var,var1)
                 break
             elif prob1 == "A" :
                 add(var,var1)
                 break
             elif prob1 == "S" :
                 sub(var,var1)
                 break
             elif prob1 == "D" : 
                 div(var,var1)
                 break
             else:
                 print "Please enter the valid option"
                 k = k + 1
                 continue
         break
     else:
          print "Please enter valid option"
          j = j + 1
          continue
     
    exitstay()
 elif (var == "intermediate"):
    print 2
    j = 0
    while(j < j+1):
     j = 0    
     ad = raw_input("Please give us the number of question you want to attempt highest is 3:")
     var1 = int(ad)
    
     if var1 <= 3 and var1 > 0 :
         print ad
         k = 0
         while(k < k+1):
             k = 0
             prob1 = raw_input("Specify the question type (multiplication:M, addition:A, subtraction:S, division:D) :")
             prob1 = str(prob1)
             if prob1 == "M" :
                 mul(var,var1)
                 break
             elif prob1 == "A" :
                 add(var,var1)
                 break
             elif prob1 == "S" :
                 sub(var,var1)
                 break
             elif prob1 == "D" : 
                 div(var,var1)
                 break
             else:
                 print "Please enter the valid option"
                 k = k + 1
                 continue
         break
     else:
          print "Please enter valid option"
          j = j + 1
          continue
      
    exitstay()
 elif (var == "hard"):
    print 3
    j = 0
    while(j < j+1):
     j = 0    
     ad = raw_input("Please give us the number of question you want to attempt highest is 3:")
     var1 = int(ad)
    
     if var1 <= 3 and var1 > 0 :
         print ad
         k = 0
         while(k < k+1):
             k = 0
             prob1 = raw_input("Specify the question type (multiplication:M, addition:A, subtraction:S, division:D) :")
             prob1 = str(prob1)
             if prob1 == "M" :
                 mul(var,var1)
                 break
             elif prob1 == "A" :
                 add(var,var1)
                 break
             elif prob1 == "S" :
                 sub(var,var1)
                 break
             elif prob1 == "D" : 
                 div(var,var1)
                 break
             else:
                 print "Please enter the valid option"
                 k = k + 1
                 continue
         break
     else:
          print "Please enter valid option"
          j = j + 1
          continue
      
    exitstay()
 else:
    print "Please enter valid option"
    main()

def exitstay():

 i = 0
 while(i < i+1): 
  inp = raw_input("Continue or Exit (continue: C,Exit: E) :" ) 
  inp = str(inp)
 
  if (inp == "C"):
     main()
  elif (inp == "E"):
     quit()
  else: 
     print "Please enter valid option"
     i = i + 1
     continue
 
def main():
    n = level("Choose level (easy, intermediate, and hard):")
    
if __name__ == "__main__": main()

