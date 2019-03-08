## create database  practical_exercise_1

hive -e "create database practical_exercise_1;" 

## import data from the MySQL Tables into Hive(Activitylog)
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--create practical_exercise_1.activitylog \
-- import \
--connect jdbc:mysql://localhost/practical_exercise_1 \
--username root \
--password-file /user/cloudera/root_pwd.txt \
--table activitylog \
-m 2 \
--hive-import \
--hive-database practical_exercise_1 \
--hive-table activitylog \
--incremental append \
--check-column id \
--last-value 0

sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--exec practical_exercise_1.activitylog 

## import data from the MySQL Tables into Hive(User) 
sqoop import \
--connect jdbc:mysql://localhost/practical_exercise_1 \
--username root \
--password cloudera \
--table user \
-m 2 \
--hive-import \
--hive-overwrite \
--hive-database practical_exercise_1 \
--hive-table user 

## make directory to hdfs 

hadoop fs -mkdir /user/cloudera/workshop/exercise1
hadoop fs -ls  /user/cloudera/workshop/   


hive -e "create table if not exists user_report(id int, total_update bigint, total_insert bigint, total_delete bigint, last_activity_type string, is_active boolean, upload_count bigint);"

hive -e "CREATE TABLE if not exists user_total (time_ran TIMESTAMP, total_users int, user_added int);" 


