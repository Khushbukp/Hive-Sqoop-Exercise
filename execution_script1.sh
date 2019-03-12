
python /home/cloudera/practical_exercise_data_generator.py --load_data

python /home/cloudera/practical_exercise_data_generator.py --create_csv

 
## executing sqoop job

sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--exec practical_exercise_1.activitylog 

## overwrititng user data  

sqoop import \
--connect jdbc:mysql://localhost/practical_exercise_1 \
--username root \
--password cloudera \
--table user \
-m 2 \
--hive-import \
--hive-overwrite \
--hive-database practical_exercise_1 \
--hive-table user \

## To ingest CSV files from the local file system into HDFS

mv  *.csv   /home/cloudera/TEMP/user_upload_dump.$(date +%s).csv
hadoop fs -put   /home/cloudera/TEMP/*.csv   /user/cloudera/workshop/exercise1    
hadoop fs -ls  /user/cloudera/workshop/exercise1/ 
mv /home/cloudera/TEMP/*.csv   /home/cloudera/backup/
ls /home/cloudera/backup/

## hive commands
  
hive -e "use practical_exercise_1;"
hive -e "show tables;"
hive -e "select * from practical_exercise_1.user;" 
hive -e "select * from practical_exercise_1.activitylog;" 

## ingesting csv files from hdfs to hive 
hive -e "CREATE EXTERNAL TABLE if not exists practical_exercise_1.user_upload_dump ( user_id int, file_name STRING, timestamp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/workshop/exercise1/' tblproperties ('skip.header.line.count' = '1');"

hive -e "select count(*) from practical_exercise_1.user_upload_dump;"

## generating user_report table  
hive -e "truncate table practical_exercise_1.user_report;" 

hive -e "insert into practical_exercise_1.user_report
Select u.id, al.total_update, al.total_insert, al.total_delete, b.Type as Last_ACTIVITY_Type, al.IS_ACTIVE, d.total_upload from practical_exercise_1.user u 
left join (select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from practical_exercise_1.activitylog) as b On u.id= b.user_id and b.row_num=1 
join (select count(*) as total_upload, user_id  from practical_exercise_1.user_upload_dump group by user_id) as d on d.user_id=u.id 
join (select user_id, count(case when type='INSERT' then 1 else NULL end)as total_insert, count(case when type='UPDATE' then 1 else NULL end) as total_update, count(case when type='DELETE' then 1 else NULL end) as total_delete, (Case when max(from_unixtime(timestamp)) >= date_sub(current_timestamp,2) then 'true' else 'false' end) as IS_ACTIVE
from practical_exercise_1.activitylog group by user_id) as al On u.id = al.user_id ; "

hive -e "select * from practical_exercise_1.user_report;"

## inserting data into user_total table   

value=$(hive -e "select count(total_users) from practical_exercise_1.user_total;")
stringarray=($value)
echo ${stringarray[0]}
count=${stringarray[0]}
echo $count
if [ $count -gt 0 ]; then
echo "if part"
hive -e "insert into practical_exercise_1.user_total select CURRENT_TIMESTAMP, TT ,(TT - OLD ) from (select CURRENT_TIMESTAMP, count(user.id) as TT,user_total.TOTAL_USERS as OLD,MAX(user_total.TIME_RAN) as MT from practical_exercise_1.user ,practical_exercise_1.user_total  group by user_total.TIME_RAN,user_total.TOTAL_USERS order by MT desc) t1;"
else
echo "else part"
hive -e "insert into practical_exercise_1.user_total select CURRENT_TIMESTAMP, count(a.id), count(a.id) from practical_exercise_1.user a;"
fi

hive -e "select * from practical_exercise_1.user_total;"

