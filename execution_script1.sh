python /home/cloudera/practical_exercise_data_generator.py --load_data

python /home/cloudera/practical_exercise_data_generator.py --create_csv

 ##steps:  some more changes:
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
hadoop fs -put   /home/cloudera/TEMP/*.csv   /user/cloudera/exercise/    
hadoop fs -ls  /user/cloudera/exercise/ 
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

hive -e "insert into user_report Select u.id,c.TOTAL_UPDATE, c.TOTAL_INSERT,  c.TOTAL_DELETE, b.Type as Last_ACTIVITY_Type, c.IS_ACTIVE,  d.total_upload from user u
Join(select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from activitylog) as b On u.id= b.user_id and b.row_num=1
join (SELECT user_id, count(case when type='DELETE' then 1 else NULL end) as TOTAL_DELETE, count(case when type='UPDATE' then 1 else NULL end) as TOTAL_UPDATE,
count(case when type='INSERT' then 1 else NULL end) as TOTAL_INSERT, case when count(case when timestamp > date_sub(current_date, 2) THEN 1 ELSE NULL END) >= 1 then 'true' else 'false' end as IS_ACTIVE FROM activitylog  group by user_id) as c on u.id=c.user_id Join (select count(*) as total_upload, user_id  from user_upload_dump  group by user_id) as d on d.user_id=u.id ;"

hive -e "select * from user_report;"

## inserting data into user_total table   
hive -e "insert into user_total select CURRENT_TIMESTAMP, count(a.id), count(a.id) from user a;"   

hive -e "insert into user_total select CURRENT_TIMESTAMP, TT , (case when (TT - OLD)=0 then TT else (TT - OLD) end) from ( 
select row_number() over(order by b.TIME_RAN DESC) as RN,CURRENT_TIMESTAMP,count(a.id) as TT,b.TOTAL_USERS as OLD,MAX(b.TIME_RAN) as MT from user a,user_total b group by b.TIME_RAN,b.TOTAL_USERS order by MT desc ) t1  where RN = 1;" 

hive -e "select * from user_total";

