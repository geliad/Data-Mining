package Georgios

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._

object Task1 {
  
  def parseLine(line:String)= {
        val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
        val country = fields(3).toString
        val salary = fields(52) // exei pou oulla
        val salaryType = fields(53) // exei diafora
        (country, salary, salaryType)
    }
  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Task1")  
    
    //read file
    val input = sc.textFile("survey_results_public.csv")
    val header = input.first()
    val removeColumnNames = input.filter(row => row!= header)
    
    //Get only the columns we want
    val parsedLines = removeColumnNames.map(parseLine)
    
    //Filter NA and 0 values
    val reportedSalaryValues = parsedLines.filter(x => x._2 != "NA")
    val reportedSalaryValues2 = reportedSalaryValues.filter(x => x._2 != 0)
    val reportedSalaryValues3 = reportedSalaryValues2.filter(x => x._2 != " ")
    val reportedSalaryValues4 = reportedSalaryValues3.filter(x => x._2 != "0")
        
    // (Germany), (Cyprus)...
    val monoKeyCountry = reportedSalaryValues4.map(x => (x._1)).map(_.trim.replace("\"", ""))

    // (Germany , 1), (Cyprus, 1), (Germany, 2)
    val reportedSalaryValues2rdd = monoKeyCountry.map(x => (x, 1))
   
    val count = reportedSalaryValues2rdd.reduceByKey( (x,y) => x + y )
    val s = count.sortByKey()  
    val results = s.collect()
  
    //Calculate Total 
    val datasets = sc.parallelize(results) 
    val x = datasets.values.sum().toInt
    /*
     * PRINT RESULTS
    println(f"Total,$x")
    for (res <- results) {
        val couN = res._1
        val salA = res._2
        println(s"$couN,$salA")
    }
    */
    //Write to CSV
    val neo = sc.parallelize(Seq(("Total",x))).collect()
    val finalResults = neo.union(results)
    //finalResults.foreach(println)
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val end = spark.createDataFrame(finalResults).toDF()
    end.coalesce(1).write.csv("Georgios_Iliadis_task1.csv")
    
  }
}