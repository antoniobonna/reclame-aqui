import os
import re
import psycopg2
import csv
from collections import Counter
import credentials
from nltk.tokenize import word_tokenize,sent_tokenize
from nltk.corpus import stopwords
from nltk.util import ngrams
import psycopg2
import credentials
from subprocess import call
import enchant

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

stopwords = stopwords.words('portuguese')
p = enchant.Dict("pt_BR")
e = enchant.Dict()
nonstopwords = ['intermedium','isafe','sms','xiaomi','nfc','sicoob','wifi','cdb','redmi','ios','mei','crashar','nuconta','samsung','iphone','aff','ok','credicard','criptomoedas','cdbs','bugado','itoken','broker','fintech','fintechs','superapp','instagram','facebook','whatsapp','blackfriday','friday']

def words(text):
    pattern = re.compile(r"[^\s]+")
    non_alpha = re.compile(r"[^\w]", re.IGNORECASE)
    for match in pattern.finditer(text):
        nxt = non_alpha.sub("", match.group()).lower()
        return nxt

### variaveis
outdir = '/home/ubuntu/scripts/load-dados-reclame-aqui/csv/'
file = 'trigram_nao_avaliadas.csv'
query_app = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE reclamacoes_nao_avaliadas = True AND empresa_id in ('lopes-imoveis','quinto-andar','zap-imoveis', 'enjoei')"
query_classificacao = "SELECT DISTINCT classificacao FROM reclame_aqui_dw.vw_reclamacoes_nao_avaliadas WHERE empresa_id = '{}'"
query_comentario = "SELECT unaccent(reclamacao) reclamacao FROM reclame_aqui_dw.vw_reclamacoes_nao_avaliadas WHERE empresa_id = '{}' AND classificacao = '{}'"
tablename = 'reclame_aqui_dw.trigrams_reclamacoes_real_state'

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')

cursor.execute(query_app)
companies = [item[0].lower() for item in cursor.fetchall()]
nonstopwords = nonstopwords + companies

with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
    writer = csv.writer(ofile, delimiter=';')

    cursor.execute(query_app)
    apps = [item[0] for item in cursor.fetchall()]
    for app in apps:
        #try:
            print('Parsing '+app+'...')
            cursor.execute(query_classificacao.format(app))
            datas = [item[0] for item in cursor.fetchall()]
            for classificacao in datas:
                cursor.execute(query_comentario.format(app,classificacao))
                comments = [str(item[0]) for item in cursor.fetchall()]
                ltrigrams = []
                for comment in comments:
                    sentence = [words(word) for word in word_tokenize(comment,language='portuguese')]
                    sentence = [x.replace('oq','que').replace('vcs','vocês').replace('vc','você').replace('funcao','função').replace('notificacoes','notificações').replace('hj','hoje').replace('pq','porque').replace('msm','mesmo').replace('td','tudo').replace('vzs','vezes').replace('vlw','valeu').replace('msg','mensagem').replace('mt','muito') for x in sentence if x]
                    sentence = [x for x in sentence if x not in stopwords]
                    trigrams=ngrams(sentence,3)
                    ltrigrams += list(trigrams)
                counter = Counter(ltrigrams)

                for trigram,count in counter.most_common():
                    if trigram and count > 1:
                        trigram = '_'.join(trigram)
                        writer.writerow([app,classificacao,trigram.rstrip('_'),count])
        # except:
            # pass
### truncate
query = "TRUNCATE " + tablename
cursor.execute(query)
db_conn.commit()

## copy
with open(outdir+file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()
os.remove(outdir+file)

# # ### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)
