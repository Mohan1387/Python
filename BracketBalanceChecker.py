l='[()]{}{[()()]()}'

def isdiffcheck(a,b):
    if(a == '{') and (b == '}'):        
        return True    
    elif(a == '[') and (b == ']'):        
        return True    
    elif(a == '(') and (b == ')'):        
        return True

def balanceChecker(st):    
    s = []    
    balanced = True    
    index = 0    
    while index < len(st) and balanced == True:        
        char_st = st[index]        
        print(st[index])        
        if char_st == "{" or char_st == "[" or char_st == "(":            
            s.append(char_st)        
        else:            
            if len(s) == 0:               
                balanced = False            
            else:                
            if(isdiffcheck(s.pop(),char_st) != True):                    
                balanced = False        
    index += 1
    if balanced and len(s) == 0:        
        return True    
    else:        
        return False
        
print(balanceChecker(l))
