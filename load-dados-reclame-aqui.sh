#!/bin/bash

### Definicao de variaveis
LOG="/var/log/scripts/scripts.log"
PARSE_BADFILE="/home/ubuntu/scripts/load-dados-reclame-aqui/csv/parse_error.log"
DIR="/home/ubuntu/scripts/load-dados-reclame-aqui"
DUMP="/home/ubuntu/dump"
STARTDATE=$(date +'%F %T')
SCRIPTNAME="load-dados-reclame-aqui.sh"
BOT="bot_message.py"
source "/home/ubuntu/scripts/load-dados-covid-19/credentials.sh"

horario()
{
	date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

stagingDados()
{
	FILE=$1
	cd ${DIR}/reclame_aqui

	time (scrapy crawl $FILE -o ${FILE}.csv
	[ -r ${FILE}.csv ] && pg_bulkload -h $HOST -U $USER -d $DATABASE -i ${FILE}.csv -P $PARSE_BADFILE -o "SKIP=1" -O reclame_aqui.${FILE}_stg $BULKLOAD_CONTROL
	psql -d $DATABASE -c "VACUUM ANALYZE reclame_aqui.${FILE}_stg;"

	rm -f ${FILE}.csv)
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f stagingDados

LoadDW()
{
	FILE=$1
	time psql -d torkcapital -f ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f LoadDW

LoadHist()
{
	TABLE=$1
	time (psql -d torkcapital -c "COPY (SELECT DISTINCT * FROM reclame_aqui.${TABLE}_stg) TO '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "COPY reclame_aqui.${TABLE}_hist FROM '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "VACUUM ANALYZE reclame_aqui.${TABLE}_hist;"
	psql -d torkcapital -c "TRUNCATE reclame_aqui.${TABLE}_stg;")
	echo -e "$(horario): Tabela $TABLE transferida para dados historicos.\n-\n"
}
export -f LoadHist

python ${DIR}/${BOT} START DAILY

### Carrega arquivos nas tabelas staging

echo -e "$(horario): Inicio do staging.\n-\n"

ID=$(sudo docker run -it -p 8050:8050 -d scrapinghub/splash)

ListaArquivos="reclamacoes reclamacoes_nao_avaliadas"
for FILE in $ListaArquivos; do
	stagingDados $FILE
done

### Carrega dados no DW

echo -e "$(horario): Inicio da carga no DW.\n-\n"

ListaArquivos="etl_reputacao.sql etl_reclamacoes_avaliadas.sql etl_reclamacoes_nao_avaliadas.sql"
for FILE in $ListaArquivos; do
	LoadDW $FILE
done

### Limpa tabelas staging e carrega no historico

echo -e "$(horario): Inicio da limpeza do staging.\n-\n"

ListaTabelas="reclamacoes reputacao"
for TABLE in $ListaTabelas; do
	LoadHist $TABLE
done

### Remove arquivos temporarios e escreve no log

rm -f ${DUMP}/*.txt

ENDDATE=$(date +'%F %T')
echo "$SCRIPTNAME;$STARTDATE;$ENDDATE" >> $LOG

echo -e "$(horario):Fim da execucao.\n"

sudo docker stop $ID

python ${DIR}/${BOT} "END"

exit 0
