#!/bin/sh

start=`date '+%Y-%m-%d %H:%M:%S'`
filepath=/usr/lib/hbase/lib
for i in `ls /usr/lib/hbase/lib/*.jar`
do
        LIBJARS=$i,$LIBJARS
        HADOOPCLASSPATH=$i:$HADOOPCLASSPATH
done

data_root=/data

export LIBJARS=$LIBJARS
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOPCLASSPATH

echo "The input local path :  $1"
filepath="$data_root/txt"
echo "Step 0 : The HDFS data root path : $filepath"
list_alldir(){
  for file2 in `ls $1`
  do
     if [ x"$file2" != x"." -a x"$file2" != x".." ];then
         if [ -d "$1/$file2" ];then
            echo "Step 0 : ################## $file2"
            filepath="$filepath/$file2"
         fi
     fi
  done
}


list_1level_dir(){
for file2 in `ls -a $1`
do
   if [ x"$file2" != x"." -a x"$file2" != x".." ];then
      if [ -d "$file2" ];then		
echo "Setp 0-1 : ###########################################################"
		echo "Setp 0-2 : $start: Import data to HBase table [$1]"
		echo "Setp 0-3 : HBase Table Name : FSN_$file2"
		tableName="FSN_$file2"
		inputFilePath="$1/$file2"
		echo "Setp 0-4 : input : $inputFilePath"
		echo "Setp 0-5 : The HDFS file path : $filepath"
		list_alldir $inputFilePath/txt
		echo "Setp 0-6 : $file2"
		chmod -R 777 $inputFilePath/txt
		chmod -R 777 $inputFilePath/zip

		echo "Setp 0-7 : The HDFS file path : $filepath"
		copyStart=`date '+%Y-%m-%d %H:%M:%S'`
		echo "Setp 0-8 : $copyStart : Copy the local file $inputFilePath/txt and $inputFilePath/zip to the HDFS $data_root"
		bash import.sh $inputFilePath $data_root $filepath $tableName
		filepath="$data_root/txt"
     fi
  fi
done
}
list_1level_dir $1
