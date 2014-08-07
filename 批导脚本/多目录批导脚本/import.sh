#!/bin/sh

echo "Setp Start : param-1 : $1"
echo "Setp Start : param-2 : $2"
echo "Setp Start : param-3 : $3"
echo "Setp Start : param-4 : $4"

zk=dp24,dp25,dp23
zkport=2181
zk_node_parent=hbase-unsecure
startKey=AAAA0000
endKey=ZZZZ9999
numRegions=7

starttime=$(date +%s)
copyStartTime=$(date +%s)
copyStart=`date '+%Y-%m-%d %H:%M:%S'`
echo "Setp 1 : copyStart : Copy the local file $1/txt and $1/zip to the HDFS $2"
su hbase <<EOF
echo "Setp 1-1 : Copy the local file $1/txt to the HDFS $2"
hadoop fs -copyFromLocal $1/txt $2

echo "Step 1-2 : Copy the local file $1/zip to the HDFS $2"
hadoop fs -copyFromLocal $1/zip $2
echo "Step 1-3 : Copy the local file $1/zip to the HDFS $2 Completed"
EOF
copyEndTime=$(date +%s)
copyEnd=`date '+%Y-%m-%d %H:%M:%S'`
echo "Setp 1-4 : $copyEnd : Copy the local file $1/txt and $1/zip to the HDFS $data_root Completed,Copy File time-consuming :  $((copyEndTime-copyStartTime)) Second"
su hdfs <<EOF
	echo "Setp 2 : Directory authorization :  $2"
	hadoop fs -chmod -R 777 $2
EOF

echo 'Setp 3 : The M/R input file path : $3'
importStartTime=$(date +%s)
importStartDate=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step 3-1 : $importStartDate : Start bulk load data to hbase table"

su hbase <<EOF
	hadoop jar hbase-bulkload.jar $3 /tmp/$4 $4 $zk $zkport $zk_node_parent $startKey $endKey $numRegions
	echo "Setp 3-2 : Import hbase data success"
	echo "Setp 3-3 : Delete Mapreduce generated in the middle of the file"
	hadoop fs -rm -r /tmp/$4
EOF
importEndTime=$(date +%s)
importEndDate=`date '+%Y-%m-%d %H:%M:%S'`
echo "Setp 3-5 : $importEndDate : Bulk load data to hbase table $4 completed,Import data time-consuming: $((importEndTime-importStartDate)) Second"

echo "Setp 4 : Delete local file :$1"
rm -rf $1
endtime=$(date +%s)
end=`date '+%Y-%m-%d %H:%M:%S'`
echo "Setp 4-1 : $end: Import data to HBase table [$4] Completed"
echo "Setp 4-2 Import data total time-consuming : $((endtime-starttime)) Second" 
