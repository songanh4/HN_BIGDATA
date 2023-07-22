#! /bin/bash

WORK_DIR=/solr/solr_script/account_extend_info/
DAY_KEY=$(date -d "$date -1 month" +%Y%m%d)
FIRST_DAY_PRE_MONTH=${DAY_KEY:0:6}01
LAST_DAY_PRE_MONTH=$(date -d "$FIRST_DAY_PRE_MONTH + 1 month - 1 day" +%Y%m%d)

LOG_LOCATION=/solr/solr_script/account_extend_info/logs/rerun/
exec > >(tee -i $LOG_LOCATION/script_${LAST_DAY_PRE_MONTH}.log)
exec 2>&1

echo 'Rerun account_extend_info 07 every month'
echo 'DAY_KEY:'$DAY_KEY
echo $FIRST_DAY_PRE_MONTH
echo $LAST_DAY_PRE_MONTH

sh $WORK_DIR/account_extend_info.sh $LAST_DAY_PRE_MONTH    

