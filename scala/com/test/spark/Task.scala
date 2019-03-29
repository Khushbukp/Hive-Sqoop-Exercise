package com.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object Task {
   def main(args:Array[String]) {
     val conf = new SparkConf().setAppName("spark_task").setMaster("local")
     val sc = new SparkContext(conf)
     val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    
     val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
     //here, using show() to check the table on console for testing only... 
    val user_upload_dumpdf = hqlContext.sql("select * from practical_exercise_1.user_upload_dump").show()
    val userdf = hqlContext.sql("select * from practical_exercise_1.user").show()
    val activitylogdf = hqlContext.sql("select * from practical_exercise_1.activitylog").show()
    
    // gerenarting user_report table 
    val user_report = hqlContext.sql("Select u.id, al.total_update, al.total_insert, al.total_delete, b.Type as Last_ACTIVITY_Type, al.IS_ACTIVE, d.total_upload from practical_exercise_1.user u left join (select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from practical_exercise_1.activitylog) as b On u.id= b.user_id and b.row_num=1 join (select count(*) as total_upload, user_id  from practical_exercise_1.user_upload_dump group by user_id) as d on d.user_id=u.id join (select user_id, count(case when type='INSERT' then 1 else NULL end)as total_insert, count(case when type='UPDATE' then 1 else NULL end) as total_update, count(case when type='DELETE' then 1 else NULL end) as total_delete, (Case when max(from_unixtime(timestamp)) >= date_sub(current_timestamp,2) then 'true' else 'false' end) as IS_ACTIVE from practical_exercise_1.activitylog group by user_id) as al On u.id = al.user_id ").registerTempTable("userReportTemp")
  
    //query on Temptable which is stored in hive 
    val result = hqlContext.sql("select * from userReportTemp").show()
    
   }
}