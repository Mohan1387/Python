from time import sleep
from urllib.parse import urlparse
from urllib.parse import parse_qs
import csv
from urllib.request import urlopen
import shutil
import os.path
import datetime
import pandas as pd
import random
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC



def get_whois(domain_name):
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": r"stocks",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    try:
        driver.get("https://mxtoolbox.com/SuperTool.aspx?action=whois%3a{}&run=toolpage".format(domain_name))
        sleep(10)
        element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, "content"))
        )
        tbl = element.find_elements_by_xpath("//table[@class='table table-striped table-bordered table-condensed tool-result-table']")
        tr_lst = [t.find_elements_by_xpath(".//td") for t in tbl[:-1]]
        lst = [t.text for td in tr_lst for t in td]
        var = dict()
        for i in range(0, len(lst), 2):
            if i < len(lst):
                try:
                    if var[lst[i]]:
                        if isinstance(var[lst[i]], list):
                            var[lst[i]] = var[lst[i]] + [lst[i + 1]]
                        else:
                            var[lst[i]] = [var[lst[i]], lst[i + 1]]
                except:
                    var[lst[i]] = lst[i + 1]
        if len(var) > 1:
            driver.quit()
            return var
        else:
            al_var = {'Registrar': '-', 'Name Server': '-', 'Domain Name': domain_name,
                      'Registry Domain ID': '-', 'Registrar WHOIS Server': '-', 'Registrar URL': '-',
                      'Updated Date': '-', 'Creation Date': '-', 'Registry Expiry Date': '-',
                      'Registrar IANA ID': '-', 'Registrar Abuse Contact Email': '-',
                      'Registrar Abuse Contact Phone': '-',
                      'Domain Status': '-', 'DNSSEC': '-', 'URL of the ICANN Whois Inaccuracy Complaint Form': '-',
                      'Last update of whois database': '-'}
            driver.quit()
            return al_var
    except:
        driver.quit()


result = get_whois('digital14.com')


print(result)
sleep(random.randint(5, 10))







'''    
    
    dv = driver.find_elements_by_xpath("//div[@class='tool-result-body']")
    print(dv)
    tbl = [el for el in dv if el.find_elements_by_xpath('.//table')]
    print(tbl)
    tr_lst = [t.find_elements_by_xpath(".//td") for t in dv]
    lst = [t.text for td in tr_lst for t in td]
    print(lst)
    var = dict()
    for i in range(0, len(lst), 2):
        if i < len(lst):
            try:
                if var[lst[i]]:
                    if isinstance(var[lst[i]], list):
                        var[lst[i]] =  var[lst[i]] + [ lst[i+1] ]
                    else:
                        var[lst[i]] = [var[lst[i]], lst[i + 1]]
            except:
                var[lst[i]] = lst[i+1]
    return var


    if len(var) != 0:
        return var
    else:
        al_var = {'Registrar': '-', 'Name Server': '-', 'Domain Name': domain_name,
                  'Registry Domain ID': '-', 'Registrar WHOIS Server': '-', 'Registrar URL': '-',
                  'Updated Date': '-', 'Creation Date': '-', 'Registry Expiry Date': '-',
                  'Registrar IANA ID': '-', 'Registrar Abuse Contact Email': '-', 'Registrar Abuse Contact Phone': '-',
                  'Domain Status': '-', 'DNSSEC': '-', 'URL of the ICANN Whois Inaccuracy Complaint Form': '-',
                  'Last update of whois database': '-'}
        return al_var

'''


"""
for tr in mydriver.find_elements_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_lblann"]/table//tr'):
    tds = tr.find_elements_by_tag_name('td')
    print ([td.text for td in tds])
    
def get_crumb():
    driver.get("https://mxtoolbox.com/SuperTool.aspx?action=whois%3achalhoubgroup.com&run=toolpage")
    sleep(10)
    el = driver.find_element_by_xpath("// a[. // span[text() = 'Download Data']]")
    link = el.get_attribute("href")
    a = urlparse(link)
    crumb = parse_qs(a.query)["crumb"][0]
    return crumb
"""
