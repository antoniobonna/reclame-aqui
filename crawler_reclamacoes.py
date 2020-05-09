# -*- coding: utf-8 -*-
"""
Created on Fri May  1 22:13:46 2020

@author: anton
"""

from bs4 import BeautifulSoup as bs
from selenium import webdriver
from datetime import datetime
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import csv
import psycopg2
import credentials
# from pyvirtualdisplay import Display
from functools import partial
from multiprocessing import Pool
import logging
from os import remove
from subprocess import call

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
reclameaqui_url = "https://www.reclameaqui.com.br"
base_url = "https://www.reclameaqui.com.br/empresa/{}/lista-reclamacoes/?pagina={}"
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
outdir = '/home/ubuntu/scripts/load-dados-reclame-aqui/csv/'
logfile = 'reclameaqui_nao_avaliadas.log'
log_format = '%(asctime)s : %(levelname)s : %(message)s'
logging.basicConfig(filename=outdir+logfile, level=logging.WARNING, format=log_format)
logger = logging.getLogger()
file = 'comentarios.csv'
tablename = 'reclame_aqui.reclamacoes_nao_avaliadas_stg'
WAIT = 60

def _Chrome(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)
    return driver

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')
    return (db_conn,cursor)

def getFields(fields):
    cidade = fields[0].text.split('-')[0].strip()
    estado = fields[0].text.split('-')[-1].strip()
    if len(estado) > 2:
        estado = None
    id = fields[1].text.split()[-1]
    str_datetime = fields[2].text.strip()
    current_datetime = str(datetime.strptime(str_datetime, '%d/%m/%y às %Hh%M'))
    return (cidade,estado,id,current_datetime)

def writeCSV(row,file):
    with open(file,'a', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        writer.writerow(row)

def parseComplain(driver,link,empresa,file):
    driver.get(link)
    element_present = EC.presence_of_element_located((By.CLASS_NAME,'complain-body'))
    WebDriverWait(driver, WAIT).until(element_present)
    bs_page = bs(driver.page_source, 'html.parser')
    title = bs_page.find('h1', {'class': 'ng-binding'}).text
    fields = bs_page.find('ul', {'class': 'local-date list-inline'}).find_all('li')
    cidade,estado,id,current_datetime = getFields(fields)
    complain = bs_page.find('div', {'class': 'complain-body'}).find('p').text
    row = [empresa,title,cidade,estado,id,current_datetime,complain,link]
    writeCSV(row,file)

def getURLS(driver):
    element_present = EC.presence_of_element_located((By.CLASS_NAME,'complain-list'))
    WebDriverWait(driver, WAIT).until(element_present)
    bs_obj = bs(driver.page_source, 'html.parser')
    boxes = bs_obj.find_all(class_='complain-list')[0].find_all('li')
    href_links = [reclameaqui_url + box.find('a').get('href') for box in boxes]
    return href_links

def parseEmpresa(driver_path,file,empresa):
    urls = []
    driver = _Chrome(driver_path)
    for i in range(1,4):
        try:
            driver.get(base_url.format(empresa,str(i)))
            urls += getURLS(driver)
        except Exception as e:
            logger.warning('Failed to get all urls from '+ empresa +': '+ str(e))
            pass
    for link in urls:
        try:
            parseComplain(driver,link,empresa,file)
        except Exception as e:
            logger.error(f'Failed to parse url from {empresa} ({link}): {str(e)}')
            pass
    driver.quit()

def main():
    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    query = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE reclamacoes_nao_avaliadas = True ORDER BY 1"
    cursor.execute(query)
    empresas = [item[0] for item in cursor.fetchall()]
    cursor.close()
    db_conn.close()

    pparseEmpresa = partial(parseEmpresa, driver_path, outdir+file)
    pool = Pool()
    pool.map(pparseEmpresa, empresas)
    pool.close()
    pool.join()

    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    with open(outdir+file, 'r') as ifile:
        SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
        print("Executing Copy in "+tablename)
        cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
        db_conn.commit()
    cursor.close()
    db_conn.close()
    remove(outdir+file)

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)

if __name__=="__main__":
    main()