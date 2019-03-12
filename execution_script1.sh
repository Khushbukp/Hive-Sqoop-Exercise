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
--password-file /user/cloudera/root_pwd.txt \
--table user \
-m 2 \
--hive-import \
--hive-overwrite \
--hive-database practical_exercise_1 \
--hive-table user 

## To ingest CSV files from the local file system into HDFS

mv  *.csv   /home/cloudera/TEMP/user_upload_dump.$(date +%s).csv
hadoop fs -put   /home/cloudera/TEMP/*.csv   /user/cloudera/workshop/exercise1/    
hadoop fs -ls  /user/cloudera/workshop/exercise1/ 
mv /home/cloudera/TEMP/*.csv   /home/cloudera/backup/
ls /home/cloudera/backup/


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
