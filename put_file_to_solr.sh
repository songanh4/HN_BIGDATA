#!/bin/bash
# Log Location on Server.
#LOG_LOCATION=/solr/solr_script/account_info/myscripts/logs/
#exec > >(tee -i $LOG_LOCATION/script1.log)
#exec 2>&1

if [ "$1"A = A ]
then
    echo 'First argument file_name is null or empty.'
    exit 0
else
    fileName=$1
fi

if [ "$2"A = A ]
then
    echo 'First argument collection_name is null or empty.'
    exit 0
else
    collection_name=$2
fi

echo -------------------------------------------------------------------------------------------------------------------------------------------
echo -------------------------------------------------------------------------------------------------------------------------------------------
echo ------- Put file ${fileName} to Solr Server
echo '------- StartTime: '$(date '+%Y-%m-%d %H:%M:%S')' --------'
find ${fileName} -exec curl -i -k --negotiate -u: https://wn12-cdp-prod.mobifone.local:8985/solr/$collection_name/update -H "Content-Type: application/csv" --data-binary @{} \;
echo '------- EndTime: '$(date '+%Y-%m-%d %H:%M:%S')' ----------'