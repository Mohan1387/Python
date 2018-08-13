import pandas as pd

iplist_10 = []
iplist_172 = []
iplist_192 = []


for i in range(0, 256):
    for j in range(0, 256):
        iplist_192.append('192.168.' + str(j) + '.' + str(i))
        for k in range(0, 256):
            iplist_10.append('10.'+str(k)+'.'+str(j)+'.'+str(i))
            if k in range(16, 32):
                iplist_172.append('172.'+str(k)+'.'+str(j)+'.'+str(i))

df10 = pd.DataFrame({'IP': iplist_10})
df172 = pd.DataFrame({'IP': iplist_172})
df192 = pd.DataFrame({'IP': iplist_192})

#df10.to_csv("10rangeIPs.csv", encoding='utf-8', header=True, index_label='Sl_No')
#df172.to_csv("172rangeIPs.csv", encoding='utf-8', header=True, index_label='Sl_No')
#df192.to_csv("192rangeIPs.csv", encoding='utf-8', header=True, index_label='Sl_No')

df10.to_csv("10rangeIPs.csv", encoding='utf-8', header=False)
df172.to_csv("172rangeIPs.csv", encoding='utf-8', header=False)
df192.to_csv("192rangeIPs.csv", encoding='utf-8', header=False)
