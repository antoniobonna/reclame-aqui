# -*- coding: utf-8 -*-
import scrapy
import psycopg2
import credentials
import logging
import json
from datetime import datetime
from time import sleep

class ReclamacoesSpider(scrapy.Spider):
    name = 'reclamacoes'
    allowed_domains = ['www.reclameaqui.com.br','iosite.reclameaqui.com.br', 'iosearch.reclameaqui.com.br']

    def start_requests(self):
        DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
        db_conn,cursor = self._Postgres(DATABASE, USER, HOST, PASSWORD)
        query = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE empresa_id != 'btg-pactual-digital' AND reclamacoes_avaliadas = True ORDER BY 1"
        cursor.execute(query)
        empresas = [item[0] for item in cursor.fetchall()]
        cursor.close()
        db_conn.close()
        for empresa in empresas:
            yield scrapy.Request(
                url = 'https://iosite.reclameaqui.com.br/raichu-io-site-v1/company/shortname/' + empresa,
                callback=self.parse, 
                meta = {'empresa': empresa}
            )

    def _Postgres(self, DATABASE, USER, HOST, PASSWORD):
        ### conecta no banco de dados
        db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
        cursor = db_conn.cursor()
        return db_conn,cursor

    def parse(self, response):
        empresa = response.request.meta['empresa']
        d = json.loads(response.text)
        company_id = d['id']
        for i in range(0,40,10): # (0,número de reclamações,10)
            yield scrapy.Request(
                url=f'https://iosearch.reclameaqui.com.br/raichu-io-site-search-v1/query/companyComplains/10/{str(i)}?company={str(company_id)}&evaluated=bool:true',
                callback = self.parseComplainList,
                meta = {'empresa': empresa}
                )

    def parseComplainList(self, response):
        empresa = response.request.meta['empresa']
        sleep(2)
        d = json.loads(response.text)
        d = d['complainResult']['complains']['data']
        for complain in d:
            id = complain['id']
            newurl = 'https://iosite.reclameaqui.com.br/raichu-io-site-v1/complain/public/' + id
            yield scrapy.Request(
                url=newurl,
                callback = self.parseComplain,
                meta = {'empresa': empresa}
                )

    def parseComplain(self,response):
        d = json.loads(response.text)
        empresa = response.request.meta['empresa']
        title = d['title']
        if 'userCity' in d:
            city = d['userCity']
        else:
            city = None
        if 'userState' in d:
            state = d['userState']
        else:
            state = ''
        if len(state) > 2:
            state = None
        id = d['legacyId']
        current_datetime = str(datetime.strptime(d['created'],'%Y-%m-%dT%H:%M:%S'))
        complain = d['description'].replace('<br />','\n').replace('&quot;','\"')
        score = d['score']
        yield {
                'empresa': empresa,
                'titulo': title,
                'cidade': city,
                'uf': state,
                'id': id,
                'datetime': current_datetime,
                'reclamacao': complain,
                'nota' : score
           }
