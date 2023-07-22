#! /bin/bash
solr_endpoint=http://10.3.21.75:8985/solr/account_extend_info_c/update
#filename='/solr/solr_script/account_extend_info_c/data/part-00040-dd8c03f2-3aba-4365-b5ea-8d0129a3e510-c000.csv'
filename='/solr/solr_script/account_extend_info_c/data/test.csv'
file_up_success_cnt=0
START=$(date +%s)  
echo $filename
curl -i -k --negotiate -u: ${solr_endpoint} -H "Content-Type: application/csv" --data-binary @$filename
END=$(date +%s)
pt=$((END-START))
echo $pt            
#Thoi gian put 1 file >= 1 minutes => success          
if [ $pt -ge 600 ]; then
  echo "File uploaded successfully"
  file_up_success_cnt=$((file_up_success_cnt+1))
else
  echo "Error uploading file!"
fi                
