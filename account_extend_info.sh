#! /bin/bash
if [ "$1"A = A ]
then
  DAY_KEY=$(date -d "-1 day" +%Y%m%d)
  DAY_KEY_2=$(date -d "$DAY_KEY -1 day" +%Y%m%d)
  YEAR_KEY=${DAY_KEY:0:4}
  MO_KEY=${DAY_KEY:0:6}
  MO_KEY_2=${DAY_KEY_2:0:6}
else
  DAY_KEY_IP=$1
  #DAY_KEY=$(date -d "${MO_KEY}01 +1 month -1 day" +%Y%m%d)
  DAY_KEY=$(date -d "${DAY_KEY_IP}" +%Y%m%d)
  DAY_KEY_2=$(date -d "$DAY_KEY -1 day" +%Y%m%d)
  YEAR_KEY=${DAY_KEY:0:4}
  MO_KEY=${DAY_KEY:0:6}
  MO_KEY_2=${DAY_KEY_2:0:6}    
fi

ACCOUNT_EXTEND_INFO_HDFS_PATH_FLAG=/DATAWH/FLAG/CUSTOMER_360_FLAG/${DAY_KEY}
HDFS_PATH=/DATAWH/TMP/SOLR/ACCOUNT_EXTEND_INFO/*.csv
WORK_DIR_DATA=/solr/solr_script/account_extend_info/data
WORK_DIR=/solr/solr_script/account_extend_info/
solr_endpoint=http://10.3.21.75:8985/solr/account_extend_info/update

LOG_LOCATION=/solr/solr_script/account_extend_info/logs/
exec > >(tee -i $LOG_LOCATION/script_${DAY_KEY}.log)
exec 2>&1

echo 'YEAR_KEY:'$YEAR_KEY
echo 'MO_KEY:'$MO_KEY
echo 'MO_KEY_2:'$MO_KEY_2
echo 'DAY_KEY:'$DAY_KEY
echo 'DAY_KEY_2:'$DAY_KEY_2
echo 'ACCOUNT_EXTEND_INFO_HDFS_PATH_FLAG:'$ACCOUNT_EXTEND_INFO_HDFS_PATH_FLAG

tmp=0
check=true
while $check
do
  #echo $tmp
  if [ "${tmp}" -gt "0" ] 
  then 
    sleep 60
    echo "Sleep to check _SUCCESS file in HDFS 60s!"    
  fi
  (( tmp++ ))
  #Check _SUCCESS in HDFS
  #i=0
  #array=()
  #while read line
  #do
  	#array[ $i ]="$line"      
  	#(( i++ ))
  #done < <(hdfs dfs -ls $ACCOUNT_EXTEND_INFO_HDFS_PATH/_SUCCESS) 
  
  #Check _SUCCESS in HDFS  
  hdfs dfs -test -d $ACCOUNT_EXTEND_INFO_HDFS_PATH_FLAG    
  if [ ${?} = 0 ] 
  then      
    echo "_SUCCESS file ok on HDFS"                     
    printf 'Mbf@1234' | kinit
    kinit -R admin@MOBIFONE.COM.VN
      
    #Export data csv on hdfs
    spark-submit --driver-memory 20g --num-executors 10 --executor-cores 5 --executor-memory 5g  ${WORK_DIR}account_extend_info.py app_name=ACCOUNT_EXTEND_INFO_EXP workflow_name=ACCOUNT_EXTEND_INFO_EXP mo_key=$MO_KEY
    
    #Truncate data by day_key      
    echo ---------------------------------------------------------------------
    echo "Delete data on day_key:$DAY_KEY and mo_key:$MO_KEY"
    var1="<delete><query>(day_key:$DAY_KEY) AND (mo_key:$MO_KEY)</query></delete>"
    echo "curl --cacert ${WORK_DIR}wn13.pem ${solr_endpoint} --data '$var1'"
    curl -s --cacert ${WORK_DIR}wn13.pem ${solr_endpoint} --data "<delete><query>(day_key:$DAY_KEY) AND (mo_key:$MO_KEY)</query></delete>"    
    
    #List file on hdfs path
    i=0
    array=()
    while read line
    do
    	array[ $i ]="$line"        
    	(( i++ ))
    done < <(hdfs dfs -ls $HDFS_PATH)
    
    #Get file from hdfs to local
    echo ---------------------------------------------------------------------
    echo 'Get file from hdfs to local'    
    file_cnt=0
    mkdir -p $WORK_DIR_DATA
    rm -rf $WORK_DIR_DATA/*.csv
    cd $WORK_DIR_DATA
    for (( i=0; i<${#array[@]}; i++ ))
    {    	    
      arrIN=(${array[$i]//\\t/ })	
    	hdfsFileName=${arrIN[7]}  
    	hdfs dfs -get $hdfsFileName $WORK_DIR_DATA
      echo ---------------------------------------------------------------------
    	echo ------- Get file from hdfs: $hdfsFileName $WORK_DIR_DATA
      file_cnt=$((file_cnt+1))
    }
    
    #echo '------- StartTime: '$(date '+%Y-%m-%d %H:%M:%S')' --------'
    #find $WORK_DIR_DATA/*.csv -type f | xargs -P 2 -I {} curl -i -k --negotiate -u: http://10.3.21.75:8985/solr/account_extend_info_c/update -H "Content-Type: application/csv" --data-binary @{} \;        
    #echo '------- EndTime: '$(date '+%Y-%m-%d %H:%M:%S')' ----------'      
    
    echo ---------------------------------------------------------------------
    echo "Put file from local to Solr Server"    
    file_up_success_cnt=0
    for filename in $WORK_DIR_DATA/*.csv; do
        echo ---------------------------------------------------------------------
        START=$(date +%s)  
        echo "Start put file $filename at $(date)"
        echo "  Processing..."
        curl -s -i -k --negotiate -u: ${solr_endpoint} -H "Content-Type: application/csv" --data-binary @$filename
        END=$(date +%s)
        pt=$((END-START))
        echo "Ended put file $filename at $(date) with total time: $pt (seconds)"  
        #Thoi gian put 1 file >= 1 minutes => success          
        if [ $pt -ge 120 ]; then
          echo "File uploaded successfully"
          file_up_success_cnt=$((file_up_success_cnt+1))
        else
          echo "Error uploading file!"
        fi                
        #sh $WORK_DIR/put_file_to_solr.sh $filename $COLLECTION_NAME              
    done
    #wait          
       
    #Break for loop
    check=false
    #
    rm -rf $WORK_DIR_DATA/*.csv      
    if [ ${MO_KEY} == ${MO_KEY_2} ] 
    then
      if [ $file_up_success_cnt -eq $file_cnt ]; then
        #Truncate data by day_key
        echo ---------------------------------------------------------------------
        echo "Delete data on day_key:$DAY_KEY_2 and mo_key:$MO_KEY_2"              
        var1="<delete><query>(day_key:$DAY_KEY_2) AND (mo_key:$MO_KEY_2)</query></delete>"
        echo "curl --cacert ${WORK_DIR}wn13.pem ${solr_endpoint} --data '$var1'"
        curl -s --cacert ${WORK_DIR}wn13.pem ${solr_endpoint} --data "<delete><query>(day_key:$DAY_KEY_2) AND (mo_key:$MO_KEY_2)</query></delete>"           
      else
        echo "Total file:${file_cnt}, total file uploaded success: ${file_up_success_cnt}. Please check log to rerun data."
      fi                       
    fi                          
  else 
    echo "Data has not been aggregated ACCOUNT_EXTEND_INFO day_key:"$DAY_KEY                   
  fi  
done