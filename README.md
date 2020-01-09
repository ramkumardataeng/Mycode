package org.inceptez.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.inceptez.hack.allmethods
import org.inceptez.hack.allmethods
import org.apache.spark.sql.functions.udf

case class insureclass (IssuerId:Int,IssuerId2:Int,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)
object hackathon {
  def main(args:Array[String]){
    val spark = SparkSession.builder().master("local[*]")
                .appName("hackathon")
                .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
//1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata     
    val insuredata = spark.sparkContext.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
//2. Remove the header line from the RDD contains column names.     
    val header = insuredata.first        
    val insdata = insuredata.filter(x => x != header) 
//3. Display the count and show few rows and check whether header is removed.    
    println("Count of lines with header: " + insuredata.count)
    println("Count of lines without header: " + insdata.count)
    insdata.first()
    insdata.takeOrdered(20)
//4. Remove the blank lines in the rdd.     
    val blank = ""
    val insdatanb = insdata.filter(x=> x.trim != blank)
//5. Map and split using ‘,’ delimiter.     
    val inssplit = insdatanb.map(x=> x.split(",",-1))
//6. Filter number of fields are equal to 10.  
    val inslen10 = inssplit.filter(x=>x.length ==10)
//7. Add case class namely insureclass with the field names used as per the header record in the file and apply to the above data to create schemaed RDD.    
    val inscase = inslen10.map(x=> insureclass(x(0).toInt, x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
//8. Take the count of the RDD created in step 7 and step 1 and print how many rows are removed in the cleanup process of removing fields does not equals 10.
    val inpcnt = insuredata.count
    val cleancnt = inscase.count
    println("Count of input file : " + inpcnt)
    println("Count of cleaned up file : " + cleancnt)
    println("number of lines removed in clean up : " +(inpcnt - cleancnt))
//9. Create another RDD namely rejectdata and store the row that does not equals 10 fields, and analyze why and provide your view here.    
    val rejectdata = inssplit.filter(x=> x.length != 10)
    
    println("There are no reject data, filtering rows with 10 elements is an precaution")
//10. Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2    
    val insuredata2 = spark.sparkContext.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
//11. Repeat from step 2 to 9 for this file also and create the final rdd.    
    val header2 = insuredata2.first        
    val insdata2 = insuredata2.filter(x => x != header) 
    println("Count of lines with header: " + insuredata2.count)
    println("Count of lines without header: " + insdata2.count)
    insdata2.first()
    insdata2.takeOrdered(20)
    val insdatanb2 = insdata2.filter(x=> x.trim != blank)
    
    val inssplit2 = insdatanb2.map(x=> x.split(",",-1))
    val inslen102 = inssplit2.filter(x=>x.length ==10)
    
    val inscase2 = inslen102.map(x=> insureclass(if (x(0)==blank) 999999 else x(0).toInt, if (x(1)==blank) 999999 else x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9))) 
    val inscase2a = inscase2.filter(x => 'IssuerId != 999999 || 'IssuerId2 !=999999 )
    val inpcnt2 = insuredata2.count
    val cleancnt2 = inscase2a.count
    println("Count of input file : " + inpcnt2)
    println("Count of cleaned up file : " + cleancnt2)
    println("number of lines removed in clean up : " +(inpcnt2 - cleancnt2))
    
    
    val rejectdata2 = inssplit2.filter(x=> x.length != 10 || x(0) == blank || x(1) == blank || x(7) == blank )    
    println("There are no reject data, filtering rows with 10 elements is an precaution")
