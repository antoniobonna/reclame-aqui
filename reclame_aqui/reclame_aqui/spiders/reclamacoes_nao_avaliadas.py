# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import psycopg2
import credentials
import logging
import json
from datetime import datetime


class ReclamacoesNaoAvaliadasSpider(scrapy.Spider):
    name = 'reclamacoes_nao_avaliadas'
    allowed_domains = ['www.reclameaqui.com.br','iosite.reclameaqui.com.br'] 

    script = '''
        function main(splash, args)
            splash.private_mode_enabled = false
            url = args.url
            assert(splash:go(url))
            while not splash:select('.complain-list li a') do
                splash:wait(0.1)
              end
            splash:set_viewport_full()
            return splash:html()
        end
    '''

    def start_requests(self):
        DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
        db_conn,cursor = self._Postgres(DATABASE, USER, HOST, PASSWORD)
        query = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE reclamacoes_nao_avaliadas = True ORDER BY 1"
        cursor.execute(query)
        empresas = [item[0] for item in cursor.fetchall()]
        cursor.close()
        db_conn.close()
        for empresa in empresas:
            #for i in range(1,5):
                yield SplashRequest(
                    url = f"https://www.reclameaqui.com.br/empresa/{empresa}/lista-reclamacoes/",
                    callback=self.parse, 
                    endpoint="execute", 
                    args={'lua_source': self.script},
                    meta = {'empresa': empresa, 'page' : '1'}
                )

    def _Postgres(self, DATABASE, USER, HOST, PASSWORD):
        ### conecta no banco de dados
        db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
        cursor = db_conn.cursor()
        return db_conn,cursor

    def parse(self, response):
        empresa = response.request.meta['empresa']
        links = response.xpath('//*[@class="complain-list"]/li/a')
        page = int(response.request.meta['page'])
        for link in links:
            id = link.xpath(".//@href").get().split('/')[-2].partition('_')[-1]
            newurl = 'https://iosite.reclameaqui.com.br/raichu-io-site-v1/complain/public/' + id
            yield scrapy.Request(
                url=newurl,
                callback = self.parseComplain,
                meta = {'empresa' : empresa, 'complain_url' : response.urljoin(link.xpath(".//@href").get())}
                )
            next_page = response.xpath('//li[@class="pagination-next ng-scope"]').get()
            if next_page and page <= 4:
                yield SplashRequest(
                    url=f"https://www.reclameaqui.com.br/empresa/{empresa}/lista-reclamacoes/?pagina={str(page+1)}",
                    callback=self.parse, 
                    endpoint="execute", 
                    args={'lua_source': self.script},
                    meta = {'empresa': empresa, 'page' : str(page+1)}
                )

    def parseComplain(self,response):
        d = json.loads(response.text)
        empresa = response.request.meta['empresa']
        complain_url = response.request.meta['complain_url']
        title = d['title']
        if 'userCity' in d:
            city = d['userCity']
        else:
            city = ''
        state = d['userState']
        if len(state) > 2:
            state = None
        id = d['legacyId']
        current_datetime = str(datetime.strptime(d['created'],'%Y-%m-%dT%H:%M:%S'))
        complain = d['description'].replace('<br />','\n').replace('&quot;','\"')
        yield {
                'empresa': empresa,
                'titulo': title,
                'cidade': city,
                'uf': state,
                'id': id,
                'datetime': current_datetime,
                'reclamacao': complain,
                'url' : complain_url
           }
