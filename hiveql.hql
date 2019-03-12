-- hive commands
use practical_exercise_1;

show tables;

select * from practical_exercise_1.user;

select * from practical_exercise_1.activitylog; 

-- ingesting csv files from hdfs to hive 

CREATE EXTERNAL TABLE if not exists practical_exercise_1.user_upload_dump ( user_id int, file_name STRING, timestamp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/workshop/exercise1/' tblproperties ('skip.header.line.count' = '1');

select count(*) from practical_exercise_1.user_upload_dump;

-- generating user_report table  

insert overwrite table practical_exercise_1.user_report Select u.id, al.total_update, al.total_insert, al.total_delete, b.Type as Last_ACTIVITY_Type, al.IS_ACTIVE, d.total_upload from practical_exercise_1.user u left join (select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from practical_exercise_1.activitylog) as b On u.id= b.user_id and b.row_num=1 join (select count(*) as total_upload, user_id  from practical_exercise_1.user_upload_dump group by user_id) as d on d.user_id=u.id join (select user_id, count(case when type='INSERT' then 1 else NULL end)as total_insert, count(case when type='UPDATE' then 1 else NULL end) as total_update, count(case when type='DELETE' then 1 else NULL end) as total_delete, (Case when max(from_unixtime(timestamp)) >= date_sub(current_timestamp,2) then 'true' else 'false' end) as IS_ACTIVE from practical_exercise_1.activitylog group by user_id) as al On u.id = al.user_id ;

select * from practical_exercise_1.user_report;



