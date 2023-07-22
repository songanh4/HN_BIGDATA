#! /bin/bash
solr_endpoint=http://10.3.21.75:8985/solr/account_extend_info_c/update
HTTP_CODE=$(curl -s --cacert wn13.pem ${solr_endpoint} --data '<delete><query>day_key:20230220</query></delete>' -w '%{http_code}')
echo $HTTP_CODE

#HTTP_CODE=(${HTTP_CODE[@]})
#echo $HTTP_CODE
#code=${HTTP_CODE[-1]}
#body=${HTTP_CODE[@]::${#HTTP_CODE[@]}-1} # get all elements except last
#if [ $code -eq "200" ]; then
#  echo "File uploaded successfully"
#else
#  echo "Error uploading file, response code: ${code}"
#fi

#cnt=200
#echo "total file: ${cnt}"

#file_path='/solr/solr_script/account_extend_info_c/data/part-00000-9bc364be-9aa2-4261-8f53-e774083a799c-c000.csv'
#echo $file_path
#HTTP_CODE=$(curl -i -k --negotiate -u: http://10.3.21.75:8985/solr/account_extend_info_c/update -H "Content-Type: application/csv" --data-binary @$file_path)

#file_cnt=0

#array=( one two three )
#for i in "${array[@]}"
#do
#	echo "$i"  
#  file_cnt=$((file_cnt+1))
#done

#echo $file_cnt

#startdt=$(date)

#sleep 5

#enddt=$(date)

#echo $startdt - $enddt

#echo "the difference is $(( (startdt - enddt) / 60)) minutes"

#START=$(date +%s)
#sleep 5; # Your stuff
#END=$(date +%s)
#pt=$((END-START))
#echo $pt

#if [ $pt -ge 600 ]; then
#  echo 'Ok'
#else
#  echo 'Not Ok'
#fi 

