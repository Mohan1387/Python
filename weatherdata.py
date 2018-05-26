import datetime, urllib.request
import pandas as pd
from bs4 import BeautifulSoup as bs,SoupStrainer as ss

#creare
url = "https://www.wunderground.com/history/airport/KJFK/2014/1/1/MonthlyHistory.html?req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo="
test_url = urllib.request.urlopen(url)
page = test_url.read()
test_url.close()

# Filter only table tag
straindata = ss('table')
soup = bs(page, 'html.parser', parse_only=straindata)

tab = soup.find("table", {"class":"responsive obs-table daily"})
# make the soup object iterable
#itertab = iter(soup.findChildren('tr'))
# remove header Table row
#next(itertab)

text = ''
chk = 0
rowlist = []
for row in tab.findAll('tr'):
    rowlist.append(row)

rowlist = rowlist[2:]

inx = 0
templist = []
for row in rowlist:
    text = ''
    for cell in row.findAll(["td"]):
        text = text+cell.text.strip('\n')+'&&'
    inx = inx+1
    #weather_df.loc[inx] =
    templist.append(text.replace('\t', 'NA').replace('\t', '').replace('-,', 'NA,').replace('\n', '').strip('&&').strip().split('&&'))  #.replace(',\s+', ',').replace('\s+,', ',')

weather_df = pd.DataFrame(templist, columns=['Day','Temp_high','Temp_avg','Temp_low','DewPoint_high','DewPoint_avg','DewPoint_low','Humidity_high','Humidity_avg','Humidity_low','SeaLevelPress_high','SeaLevelPress_avg','SeaLevelPress_low','Visiblity_high','Visiblity_avg','Visiblity_low','Wind_high','Wind_avg','Wind_high','Precip_sum','Events'])

weather_df['Events'] = '\"'+weather_df['Events']+'\"'

def events_correction(row):
    events = row[20]
    if len(events) != 2:
        return events.replace('NA', '')
    else:
        return events

weather_df['Events'] = weather_df.apply(events_correction, axis = 1)
weather_df['Events'] = weather_df['Events'].replace('\"\"', "NA")

def add_date(row):
    day = row[0]
    if len(day) == 1:
        day = '2014-01-0'+day
    else:
        day = '2014-01-'+day
    return day

weather_df['Day'] = weather_df.apply(add_date, axis= 1)

print(weather_df)

'''
for lst in templist:
    print(lst)
'''