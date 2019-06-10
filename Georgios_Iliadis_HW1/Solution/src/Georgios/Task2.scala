package Georgios


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object Task2 {

  def parseLine(line:String)= {
        val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
        val country = fields(3).toString
        val salary = fields(52) // exei pou oulla
        val salaryType = fields(53) // exei diafora
        (country, salary, salaryType)
    }
 
  def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[2]", "Task2")   
    
    //read file
    val input = sc.textFile("survey_results_public.csv")
    val header = input.first()
    val removeColumnNames = input.filter(row => row!= header) 
    val parsedLines = removeColumnNames.map(parseLine)
    
    val reportedSalaryValues = parsedLines.filter(x => x._2 != "NA")
    val reportedSalaryValues2 = reportedSalaryValues.filter(x => x._2 != 0)
    val reportedSalaryValues3 = reportedSalaryValues2.filter(x => x._2 != " ")
    val reportedSalaryValues4 = reportedSalaryValues3.filter(x => x._2 != "0").map(x=> (x._1.trim.replaceAll("\"", ""), x._2.trim.replace("\"", "").replace(",",""), x._3))
     
    //NUMBER OF PARTITIONS 
    val partitionNUM = reportedSalaryValues4.partitions.size
    //print("Number of partitions for the RDD built in Task 1: ")
    //print(partitionNUM)
    
    //SPLIT USING PARTITION FUNCTION
    val newData = reportedSalaryValues4.keyBy(_._1).partitionBy(new HashPartitioner(2))
    //println(newData.partitions.size)
    //PRINTS 2
    
    
    //Items per partition
    val itemsPartitionTask1 = reportedSalaryValues4.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))} 
    val PRINTED1 = itemsPartitionTask1.map(x=> x._2.toString).collect()
   
    val itemsPartitionTask2 = newData.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
    val PRINTED2 = itemsPartitionTask2.map(x=> x._2.toString).collect()

    //Duration of rdd standard
    val reportedSalaryValues5 = reportedSalaryValues4.map(x => (x._1 , x._2.toDouble))
    val start = System.currentTimeMillis()
    reportedSalaryValues5.reduceByKey((a,b) => a + b).collect()
    val end = System.currentTimeMillis()
	  val durationMilli = (end - start)
    
	  //Duration of rdd with partition function
    val newData2 = newData.map(x=> (x._1,x._2._2.toDouble))
    val startTimeMillis = System.currentTimeMillis()
    newData2.reduceByKey((a,b) => a + b).collect()
    val endTimeMillis = System.currentTimeMillis()
	  val durationMilliSeconds = (endTimeMillis - startTimeMillis)
    
	  //Write to CSV
    val neo = sc.parallelize(Seq(("standard",PRINTED1,durationMilli))).collect()
    val neo2 = sc.parallelize(Seq(("partition",PRINTED2,durationMilliSeconds))).collect()
    
    val finalResults = neo.union(neo2)
   
     val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    
    //Convert Array[String] to String using function stringify
    val TELOS = spark.createDataFrame(finalResults).toDF("type","ArrayOfString","Time")
    val fi = TELOS.withColumn("ArrayOfString", stringify(col("ArrayOfString")))
    fi.coalesce(1).write.csv("Georgios_Iliadis_task2.csv")   
    
   
    
  }
}