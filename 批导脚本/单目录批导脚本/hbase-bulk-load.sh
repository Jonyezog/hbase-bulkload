#!/bin/sh
tableName=FSN_20140727
localPathDir=/soft

starttime=$(date +%s)
start=`date '+%Y-%m-%d %H:%M:%S'`
echo "$start: Import data to HBase table [$tableName]"
filepath=/usr/lib/hbase/lib
for i in `ls /usr/lib/hbase/lib/*.jar`
do
        LIBJARS=$i,$LIBJARS
        HADOOPCLASSPATH=$i:$HADOOPCLASSPATH
done

zk=dp23,dp24,dp25
zkport=2181
zk_node_parent=hbase-unsecure
startKey=AAAA0000
endKey=ZZZZ9999
numRegions=7
data_root=/data_hbase_input

export LIBJARS=$LIBJARS
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOPCLASSPATH

echo "The input local path :  $localPathDir"
filepath="$data_root/txt"
echo "The HDFS data root path : $filepath"
list_alldir(){
  for file2 in `ls $1`
  do
     if [ x"$file2" != x"." -a x"$file2" != x".." ];then
         if [ -d "$1/$file2" ];then
            echo "################## $file2"
            filepath="$filepath/$file2"
         fi
     fi
  done
}
list_alldir $localPathDir/txt

chmod -R 777 $localPathDir/txt
chmod -R 777 $localPathDir/zip

echo "The HDFS file path : $filepath"
copyStart=`date '+%Y-%m-%d %H:%M:%S'`
copyStartTime=$(date +%s)
echo "$copyStart : Copy the local file $localPathDir/txt and $localPathDir/zip to the HDFS $data_root"
su hbase <<EOF
echo "Copy the local file to the HDFS ,Local path : $localPathDir"
echo "Copy the local file $localPathDir/txt to the HDFS $data_root"
hadoop fs -copyFromLocal $localPathDir"/txt" $data_root
echo "Copy the local file $localPathDir/txt to the HDFS $data_root Completed"
echo "Copy the local file $localPathDir/zip to the HDFS $data_root"
hadoop fs -copyFromLocal $localPathDir"/zip" $data_root
echo "Copy the local file $localPathDir/zip to the HDFS $data_root Completed"
EOF
copyEndTime=$(date +%s)
copyEnd=`date '+%Y-%m-%d %H:%M:%S'`
echo "$copyEnd : Copy the local file $localPathDir/txt and $localPathDir/zip to the HDFS $data_root Completed,Copy File time-consuming :  $((copyEndTime-copyStartTime)) Second"
su hdfs <<EOF
echo "Directory authorization :  $data_root"
hadoop fs -chmod -R 777 $data_root
EOF

echo 'The M/R input file path : $filepath'
importStartTime=$(date +%s)
importStartDate=`date '+%Y-%m-%d %H:%M:%S'`
echo "$importStartDate : Start bulk load data to hbase table"

su hbase <<EOF
hadoop jar /soft/hbase-bulkload.jar $filepath /tmp/$tableName $tableName $zk $zkport $zk_node_parent $startKey $endKey $numRegions
echo "Import hbase data success"
echo "Delete Mapreduce generated in the middle of the file"
hadoop fs -rm -r /tmp/$1
EOF
importEndTime=$(date +%s)
importEndDate=`date '+%Y-%m-%d %H:%M:%S'`
echo "$importEndDate : Bulk load data to hbase table $tableName completed,Import data time-consuming: $((importEndTime-importStartDate)) Second"

echo "Delete local file :$localPathDir/txt,$localPathDir/zip"
rm -rf $localPathDir/txt
rm -rf $localPathDir/zip
endtime=$(date +%s)
end=`date '+%Y-%m-%d %H:%M:%S'`
echo "$end: Import data to HBase table [$tableName] Completed"
echo "Import data total time-consuming : $((endtime-starttime)) Second"
